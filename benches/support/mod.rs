//! Deterministic, bounded RDF-like corpus specifications for performance work.
//!
//! The iterator yields compact numeric specifications rather than RDF strings
//! and never retains the generated corpus. A benchmark can map the numeric
//! identifiers to its own graph IDs and encoded RDF terms while applying graph
//! visibility and orphan rules from the metadata on each [`QuadSpec`].

#![allow(dead_code)]

use std::fmt;

/// Version of the deterministic generator. Bump this when its output contract
/// changes in a way that makes existing measurements incomparable.
pub const CORPUS_VERSION: &str = "performance-corpus-v1";

/// Seed used by the named performance configurations.
pub const DEFAULT_SEED: u64 = 0x4352_4151_4c45_5030;

/// Supported quad-count dimensions.
pub const REQUIRED_QUAD_COUNTS: [usize; 3] = [10_000, 1_000_000, 10_000_000];

/// Supported graph-count dimensions.
pub const REQUIRED_GRAPH_COUNTS: [usize; 3] = [1, 32, 1_000];

/// Supported duplicate-rate dimensions.
pub const REQUIRED_DUPLICATE_PERCENTS: [u8; 3] = [0, 25, 90];

/// Named quad-count constants for benchmark configuration tables.
pub const QUADS_10K: usize = 10_000;
pub const QUADS_1M: usize = 1_000_000;
pub const QUADS_10M: usize = 10_000_000;

/// Named graph-count constants for benchmark configuration tables.
pub const GRAPHS_1: usize = 1;
pub const GRAPHS_32: usize = 32;
pub const GRAPHS_1K: usize = 1_000;

/// A validated corpus description.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorpusConfig {
    pub quads: usize,
    pub graphs: usize,
    /// Percentage of output slots whose triple payload is reused from a
    /// canonical slot in another graph. The exact number is
    /// `floor(quads * duplicate_percent / 100)`.
    pub duplicate_percent: u8,
    pub seed: u64,
}

impl CorpusConfig {
    /// Validate and construct one of the supported performance dimensions.
    ///
    /// A one-graph corpus cannot contain an across-graph duplicate. Therefore
    /// every non-zero duplicate rate is rejected for `graphs == 1` rather than
    /// silently becoming an in-graph duplicate.
    pub fn new(
        quads: usize,
        graphs: usize,
        duplicate_percent: u8,
        seed: u64,
    ) -> Result<Self, CorpusConfigError> {
        if !REQUIRED_QUAD_COUNTS.contains(&quads) {
            return Err(CorpusConfigError::UnsupportedQuadCount(quads));
        }
        if !REQUIRED_GRAPH_COUNTS.contains(&graphs) {
            return Err(CorpusConfigError::UnsupportedGraphCount(graphs));
        }
        if !REQUIRED_DUPLICATE_PERCENTS.contains(&duplicate_percent) {
            return Err(CorpusConfigError::UnsupportedDuplicatePercent(
                duplicate_percent,
            ));
        }
        if graphs == 1 && duplicate_percent != 0 {
            return Err(CorpusConfigError::OneGraphCannotDuplicate {
                graphs,
                duplicate_percent,
            });
        }

        Ok(Self {
            quads,
            graphs,
            duplicate_percent,
            seed,
        })
    }

    /// The 10,000-quad configuration for a supported graph and duplicate
    /// dimension.
    pub fn quads_10k(
        graphs: usize,
        duplicate_percent: u8,
        seed: u64,
    ) -> Result<Self, CorpusConfigError> {
        Self::new(QUADS_10K, graphs, duplicate_percent, seed)
    }

    /// The 1,000,000-quad configuration for a supported graph and duplicate
    /// dimension.
    pub fn quads_1m(
        graphs: usize,
        duplicate_percent: u8,
        seed: u64,
    ) -> Result<Self, CorpusConfigError> {
        Self::new(QUADS_1M, graphs, duplicate_percent, seed)
    }

    /// The 10,000,000-quad configuration for a supported graph and duplicate
    /// dimension.
    pub fn quads_10m(
        graphs: usize,
        duplicate_percent: u8,
        seed: u64,
    ) -> Result<Self, CorpusConfigError> {
        Self::new(QUADS_10M, graphs, duplicate_percent, seed)
    }

    /// The complete valid performance matrix. The three non-zero duplicate
    /// entries for one graph are omitted because they are impossible.
    pub fn required_matrix() -> Vec<Self> {
        Self::matrix(DEFAULT_SEED)
    }

    /// The complete valid performance matrix for an explicit seed.
    pub fn matrix(seed: u64) -> Vec<Self> {
        let mut configs = Vec::with_capacity(21);
        for &quads in &REQUIRED_QUAD_COUNTS {
            for &graphs in &REQUIRED_GRAPH_COUNTS {
                for &duplicate_percent in &REQUIRED_DUPLICATE_PERCENTS {
                    if let Ok(config) = Self::new(quads, graphs, duplicate_percent, seed) {
                        configs.push(config);
                    }
                }
            }
        }
        configs
    }

    /// Number of output slots designated as duplicate slots.
    pub fn duplicate_quads(self) -> usize {
        ((self.quads as u128 * self.duplicate_percent as u128) / 100) as usize
    }

    pub fn metadata(self) -> CorpusMetadata {
        CorpusMetadata::from_config(self)
    }
}

/// Errors returned before an iterator is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorpusConfigError {
    UnsupportedQuadCount(usize),
    UnsupportedGraphCount(usize),
    UnsupportedDuplicatePercent(u8),
    OneGraphCannotDuplicate {
        graphs: usize,
        duplicate_percent: u8,
    },
}

impl fmt::Display for CorpusConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedQuadCount(count) => {
                write!(
                    f,
                    "unsupported quad count {count}; expected 10_000, 1_000_000, or 10_000_000"
                )
            }
            Self::UnsupportedGraphCount(count) => {
                write!(
                    f,
                    "unsupported graph count {count}; expected 1, 32, or 1_000"
                )
            }
            Self::UnsupportedDuplicatePercent(percent) => {
                write!(
                    f,
                    "unsupported duplicate percent {percent}; expected 0, 25, or 90"
                )
            }
            Self::OneGraphCannotDuplicate {
                graphs,
                duplicate_percent,
            } => write!(
                f,
                "duplicate percent {duplicate_percent} is impossible across {graphs} graph"
            ),
        }
    }
}

impl std::error::Error for CorpusConfigError {}

/// Whether a benchmark should expose a graph to a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GraphVisibility {
    Visible,
    Hidden,
}

impl GraphVisibility {
    pub fn is_visible(self) -> bool {
        matches!(self, Self::Visible)
    }
}

/// The role of a quad's subject in the generated corpus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityRole {
    Root,
    Linked,
    Orphan,
}

impl EntityRole {
    pub fn is_orphan(self) -> bool {
        matches!(self, Self::Orphan)
    }
}

/// Access pattern represented by a generated quad.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CorpusShape {
    SameSubjectStar,
    LongChain,
    Orphan,
    SkewedPredicateObject,
    RarePredicate,
    CommonPredicate,
}

/// Compact predicate family. The numeric value is a stable dictionary slot,
/// not an RDF term; callers choose their IRI vocabulary when loading records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredicateKind {
    Type,
    Common(u8),
    Rare(u8),
    Chain,
}

impl PredicateKind {
    pub fn is_rare(self) -> bool {
        matches!(self, Self::Rare(_))
    }

    pub fn is_common(self) -> bool {
        matches!(self, Self::Common(_))
    }
}

/// Compact object value and representation class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectSpec {
    Iri(u64),
    Literal(u64),
}

/// One compact quad description. No RDF strings are allocated by the
/// generator, and `source_ordinal` identifies the canonical output slot for
/// a requested duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuadSpec {
    pub ordinal: usize,
    pub graph: u32,
    pub visibility: GraphVisibility,
    pub role: EntityRole,
    pub shape: CorpusShape,
    pub subject: u64,
    pub predicate: PredicateKind,
    pub object: ObjectSpec,
    pub duplicate: bool,
    pub source_ordinal: Option<usize>,
}

impl QuadSpec {
    /// Return the graph-independent triple payload used for duplicate checks.
    pub fn triple_key(self) -> TripleKey {
        TripleKey {
            subject: self.subject,
            predicate: self.predicate,
            object: self.object,
        }
    }

    pub fn is_visible(self) -> bool {
        self.visibility.is_visible()
    }

    pub fn is_orphan(self) -> bool {
        self.role.is_orphan()
    }
}

/// The graph-independent payload used when checking duplicate reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TripleKey {
    pub subject: u64,
    pub predicate: PredicateKind,
    pub object: ObjectSpec,
}

/// Small analytic metadata independent of iterator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorpusMetadata {
    pub version: &'static str,
    pub seed: u64,
    pub quads: usize,
    pub graphs: usize,
    pub duplicate_percent: u8,
    pub requested_duplicate_quads: usize,
    pub visible_graphs: usize,
    pub hidden_graphs: usize,
}

impl CorpusMetadata {
    fn from_config(config: CorpusConfig) -> Self {
        let hidden_graphs = if config.graphs > 1 {
            (config.graphs + 3) / 4
        } else {
            0
        };
        Self {
            version: CORPUS_VERSION,
            seed: config.seed,
            quads: config.quads,
            graphs: config.graphs,
            duplicate_percent: config.duplicate_percent,
            requested_duplicate_quads: config.duplicate_quads(),
            visible_graphs: config.graphs - hidden_graphs,
            hidden_graphs,
        }
    }
}
