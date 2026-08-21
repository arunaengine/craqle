//! Deterministic, bounded RDF-like corpus specifications for performance work.
//!
//! The iterator yields compact numeric specifications rather than RDF strings
//! and never retains the generated corpus. A benchmark can map the numeric
//! identifiers to its own graph IDs and encoded RDF terms while applying graph
//! visibility and orphan rules from the metadata on each [`QuadSpec`].

#![allow(dead_code)]

pub mod fixture;

use std::fmt;

use craqle::{AllowAllAuthorizer, Batch, CraqleNode, GraphId, GraphPolicy, MaterializedQuadChange};

pub trait BenchWriteExt {
    fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch>;

    fn apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch>;

    fn import_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> craqle::Result<()>;

    fn delete_graph_unchecked(&self, graph: &GraphId) -> craqle::Result<()>;
}

impl BenchWriteExt for CraqleNode {
    fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch> {
        self.apply_changes(&AllowAllAuthorizer, graph, changes)
    }

    fn apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch> {
        self.apply_changes(&AllowAllAuthorizer, graph, changes)
    }

    fn import_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> craqle::Result<()> {
        self.set_graph_policy(&AllowAllAuthorizer, graph, policy)
    }

    fn delete_graph_unchecked(&self, graph: &GraphId) -> craqle::Result<()> {
        self.delete_graph(&AllowAllAuthorizer, graph)
    }
}

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
            config.graphs.div_ceil(4)
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

/// A validated corpus handle. Cloning this value does not clone any quads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeterministicCorpus {
    config: CorpusConfig,
}

impl DeterministicCorpus {
    pub fn new(config: CorpusConfig) -> Result<Self, CorpusConfigError> {
        // Re-validate so a public struct literal cannot bypass the boundary.
        CorpusConfig::new(
            config.quads,
            config.graphs,
            config.duplicate_percent,
            config.seed,
        )?;
        Ok(Self { config })
    }

    pub fn config(self) -> CorpusConfig {
        self.config
    }

    pub fn metadata(self) -> CorpusMetadata {
        self.config.metadata()
    }

    pub fn len(self) -> usize {
        self.config.quads
    }

    pub fn iter(self) -> CorpusIter {
        CorpusIter {
            config: self.config,
            next: 0,
            duplicate_quads: self.config.duplicate_quads(),
        }
    }
}

impl IntoIterator for DeterministicCorpus {
    type Item = QuadSpec;
    type IntoIter = CorpusIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Streaming corpus iterator. Its state is constant-size regardless of the
/// requested ten-million-quad configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorpusIter {
    config: CorpusConfig,
    next: usize,
    duplicate_quads: usize,
}

impl Iterator for CorpusIter {
    type Item = QuadSpec;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.config.quads {
            return None;
        }
        let ordinal = self.next;
        self.next += 1;

        let duplicate = ordinal < self.duplicate_quads;
        let source_ordinal = duplicate_source(ordinal, self.duplicate_quads, self.config.quads);
        let payload_ordinal = source_ordinal.unwrap_or(ordinal);
        let payload = payload(payload_ordinal, self.config.seed);
        let graph = match source_ordinal {
            Some(source) => duplicate_graph(
                canonical_graph(source, self.config.graphs),
                ordinal,
                self.config.graphs,
            ),
            None => canonical_graph(ordinal, self.config.graphs),
        };

        Some(QuadSpec {
            ordinal,
            graph,
            visibility: graph_visibility(self.config.graphs, graph),
            role: payload.role,
            shape: payload.shape,
            subject: payload.subject,
            predicate: payload.predicate,
            object: payload.object,
            duplicate,
            source_ordinal,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.config.quads - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for CorpusIter {}
impl std::iter::FusedIterator for CorpusIter {}

/// Return visibility metadata for one graph without constructing a graph ID.
/// Graphs 3, 7, 11, ... are hidden for multi-graph corpora; one-graph corpora
/// remain visible.
pub fn graph_visibility(graphs: usize, graph: u32) -> GraphVisibility {
    if graphs > 1 && graph % 4 == 3 {
        GraphVisibility::Hidden
    } else {
        GraphVisibility::Visible
    }
}

#[derive(Debug, Clone, Copy)]
struct Payload {
    role: EntityRole,
    shape: CorpusShape,
    subject: u64,
    predicate: PredicateKind,
    object: ObjectSpec,
}

/// Pick a source from the non-duplicate suffix. It is always in another graph
/// for the supported matrix, and the source is emitted as a canonical record.
fn duplicate_source(ordinal: usize, duplicate_quads: usize, quads: usize) -> Option<usize> {
    if ordinal >= duplicate_quads {
        return None;
    }

    let canonical_count = quads - duplicate_quads;
    Some(duplicate_quads + ordinal % canonical_count)
}

/// Assign a canonical record to a graph while retaining the advertised
/// locality. Every eight-edge star and every 48-edge chain segment is treated
/// as one unit; the other workload records are independently distributed.
fn canonical_graph(ordinal: usize, graphs: usize) -> u32 {
    const BLOCK: usize = 128;
    let local = ordinal % BLOCK;
    let block = ordinal / BLOCK;
    let group = if local < 48 {
        block * 7 + local / 8
    } else if local < 96 {
        block * 7 + 6
    } else {
        return (ordinal % graphs) as u32;
    };
    (group % graphs) as u32
}

/// Rotate a duplicate into a deterministic destination distinct from its
/// canonical source graph. The preferred graph cycles over every graph ID;
/// only a collision with the source is advanced by one, preserving coverage.
fn duplicate_graph(source_graph: u32, ordinal: usize, graphs: usize) -> u32 {
    let preferred = (ordinal % graphs) as u32;
    if preferred == source_graph {
        (preferred + 1) % graphs as u32
    } else {
        preferred
    }
}

/// Stable SplitMix-style integer mixing. It uses only wrapping operations, so
/// output is independent of allocator, hash-map, and iteration order.
fn stable_mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn stable_id(seed: u64, ordinal: usize, salt: u64) -> u64 {
    stable_mix(seed ^ (ordinal as u64).wrapping_mul(0xd6e8_feb8_6659_fd93) ^ salt)
}

fn payload(ordinal: usize, seed: u64) -> Payload {
    const BLOCK: usize = 128;
    let local = ordinal % BLOCK;
    let block = ordinal / BLOCK;
    let jitter = stable_id(seed, ordinal, 0x0050_4159_4c4f_4144);

    if local < 48 {
        // Six eight-edge stars per block. Their subjects repeat while their
        // predicates vary, making same-subject joins deterministic.
        let star = (block * 6 + local / 8) as u64;
        let subject = 0x1000_0000_0000_0000 | star;
        let predicate = if local.is_multiple_of(8) {
            PredicateKind::Type
        } else if local.is_multiple_of(3) {
            PredicateKind::Rare((jitter % 8) as u8)
        } else {
            PredicateKind::Common((local % 4) as u8)
        };
        return Payload {
            role: if local < 8 {
                EntityRole::Root
            } else {
                EntityRole::Linked
            },
            shape: CorpusShape::SameSubjectStar,
            subject,
            predicate,
            object: ObjectSpec::Iri(stable_id(seed, ordinal, 0x5354_4152)),
        };
    }

    if local < 96 {
        // Forty-eight contiguous edges per block form a long chain.
        let chain_step = (local - 48) as u64;
        let chain_node = (block as u64) * 64 + chain_step;
        return Payload {
            role: EntityRole::Linked,
            shape: CorpusShape::LongChain,
            subject: 0x2000_0000_0000_0000 | chain_node,
            predicate: PredicateKind::Chain,
            object: ObjectSpec::Iri(0x2000_0000_0000_0000 | (chain_node + 1)),
        };
    }

    if local < 112 {
        // These subjects are never used by another shape, so they are true
        // orphan entities rather than merely unindexed linked entities.
        return Payload {
            role: EntityRole::Orphan,
            shape: CorpusShape::Orphan,
            subject: 0x8000_0000_0000_0000 | stable_id(seed, ordinal, 0x4f52_5048),
            predicate: PredicateKind::Rare((jitter % 16) as u8),
            object: ObjectSpec::Literal(stable_id(seed, ordinal, 0x4f42_4a54)),
        };
    }

    if local < 120 {
        // Most values use one hot predicate-object pair; the remaining slots
        // keep a cold value so selective lookup has both hit and miss work.
        return Payload {
            role: EntityRole::Linked,
            shape: CorpusShape::SkewedPredicateObject,
            subject: 0x3000_0000_0000_0000 | stable_id(seed, ordinal, 0x0053_4b45),
            predicate: PredicateKind::Common(0),
            object: if local.is_multiple_of(4) {
                ObjectSpec::Literal(jitter % 64 + 1)
            } else {
                ObjectSpec::Literal(0)
            },
        };
    }

    Payload {
        role: EntityRole::Linked,
        shape: if local < 124 {
            CorpusShape::RarePredicate
        } else {
            CorpusShape::CommonPredicate
        },
        subject: 0x3000_0000_0000_0000 | stable_id(seed, ordinal, 0x434f_4d4d),
        predicate: if local < 124 {
            PredicateKind::Rare((jitter % 32) as u8)
        } else {
            PredicateKind::Common((jitter % 4) as u8)
        },
        object: ObjectSpec::Literal(jitter % 1024 + 1),
    }
}

pub fn star_has_common(ordinal: usize, seed: u64) -> bool {
    let star_start = ordinal - ordinal % 8;
    (star_start..star_start + 8)
        .any(|ordinal| payload(ordinal, seed).predicate == PredicateKind::Common(0))
}
