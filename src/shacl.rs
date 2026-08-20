//! Craqle-owned SHACL compilation types.

use std::sync::Arc;
use std::time::Duration;

use crate::shacl_impl::model::CompiledSchemaInner;
use crate::{CrateViolation, EncodedTerm, QueryCancellation, ReadStatistics, RoCrateVersion};

/// SHACL feature profile implemented by Craqle's native validator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ShaclProfile {
    CraqleFastV1,
}

/// Options used while compiling a shapes graph.
#[derive(Clone, Debug, Default)]
pub struct ShaclCompileOptions {
    pub rocrate_version: RoCrateVersion,
    pub allow_local_imports: bool,
}

/// Work performed by one shapes compilation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShaclCompileStatistics {
    pub cache_hit: bool,
    pub shape_graphs: usize,
    pub shape_triples: usize,
    pub parse_time: Duration,
    pub compile_time: Duration,
}

/// Limits and cancellation state for one native validation execution.
#[derive(Clone, Debug)]
pub struct ShaclValidationOptions {
    pub cancellation: QueryCancellation,
    pub max_results: usize,
    pub max_path_edges: u64,
    pub max_path_depth: usize,
}

impl Default for ShaclValidationOptions {
    fn default() -> Self {
        Self {
            cancellation: QueryCancellation::new(),
            max_results: 10_000,
            max_path_edges: 1_000_000,
            max_path_depth: 128,
        }
    }
}

/// Work performed by one native SHACL validation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShaclValidationStatistics {
    pub shape_compile_cache_hit: bool,
    pub shapes_considered: u64,
    pub shapes_executed: u64,
    pub shapes_skipped: u64,
    pub focus_nodes: u64,
    pub target_candidates: u64,
    pub path_index_seeks: u64,
    pub path_candidate_quads: u64,
    pub constraints_evaluated: u64,
    pub terms_decoded: u64,
    pub violations: u64,
    pub full_graph_fallbacks: u64,
    pub parse_time: Duration,
    pub compile_time: Duration,
    pub target_time: Duration,
    pub constraint_time: Duration,
    pub report_time: Duration,
    pub read: ReadStatistics,
    pub stopped_early: bool,
}

/// One localized SHACL validation message.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ShaclMessage {
    pub language: Option<String>,
    pub text: String,
}

/// One deterministic native SHACL validation result.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ShaclValidationResult {
    pub focus_node: EncodedTerm,
    pub value: Option<EncodedTerm>,
    pub result_path: Option<String>,
    pub source_shape: EncodedTerm,
    pub source_constraint_component: String,
    pub severity: EncodedTerm,
    pub messages: Vec<ShaclMessage>,
}

/// Complete native SHACL validation report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShaclValidationReport {
    pub conforms: bool,
    pub results: Vec<ShaclValidationResult>,
    pub statistics: ShaclValidationStatistics,
}

impl ShaclValidationReport {
    pub fn crate_violations(&self) -> Vec<CrateViolation> {
        self.results
            .iter()
            .map(|result| CrateViolation {
                code: "shacl_violation",
                message: result
                    .messages
                    .first()
                    .map(|message| message.text.clone())
                    .unwrap_or_else(|| {
                        format!(
                            "{} failed {}",
                            result.focus_node.0, result.source_constraint_component
                        )
                    }),
                pointer: result
                    .result_path
                    .clone()
                    .unwrap_or_else(|| result.focus_node.0.clone()),
                entity_id: result
                    .focus_node
                    .to_named_node()
                    .map(|node| node.to_string()),
            })
            .collect()
    }
}

/// Immutable, portable SHACL plan produced through Rudof's parser.
#[derive(Clone, Debug)]
pub struct CompiledShaclSchema {
    pub(crate) inner: Arc<CompiledSchemaInner>,
    pub(crate) statistics: ShaclCompileStatistics,
    pub(crate) shape_versions: Arc<[(crate::GraphId, [u8; 32])]>,
}

impl CompiledShaclSchema {
    pub fn schema_hash(&self) -> [u8; 32] {
        self.inner.schema_hash
    }

    pub fn plan_fingerprint(&self) -> [u8; 32] {
        self.inner.plan_fingerprint()
    }

    pub fn model_version(&self) -> u32 {
        self.inner.format_version
    }

    pub fn rocrate_version(&self) -> RoCrateVersion {
        self.inner.rocrate_version
    }

    pub fn profile(&self) -> ShaclProfile {
        ShaclProfile::CraqleFastV1
    }

    pub fn shape_count(&self) -> usize {
        self.inner.shapes.len()
    }

    pub fn statistics(&self) -> &ShaclCompileStatistics {
        &self.statistics
    }

    pub(crate) fn shape_versions(&self) -> &[(crate::GraphId, [u8; 32])] {
        &self.shape_versions
    }
}

/// SHACL shape parsing and compilation failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ShaclError {
    #[error("SHACL shapes graph `{graph}` does not exist")]
    ShapesGraphNotFound { graph: String },
    #[error("ill-formed SHACL shapes in `{graph}`: {message}")]
    IllFormedShapes { graph: String, message: String },
    #[error("unsupported SHACL component `{component}` on shape `{shape}`")]
    UnsupportedComponent { shape: String, component: String },
    #[error("owl:imports is disabled for `{graph}` (requested `{import}`)")]
    ImportsDisabled { graph: String, import: String },
    #[error("owl:imports target `{import}` from `{graph}` is not a local Craqle graph")]
    ImportNotLocal { graph: String, import: String },
    #[error("owl:imports cycle: {graphs:?}")]
    ImportCycle { graphs: Vec<String> },
    #[error("RDF-star term is unsupported in SHACL Performance v1: {term}")]
    UnsupportedRdfStarTerm { term: String },
    #[error("SHACL data graph `{graph}` does not exist")]
    DataGraphNotFound { graph: String },
    #[error("SHACL validation was cancelled")]
    ValidationCancelled,
    #[error("SHACL path edge budget {limit} was exhausted")]
    PathBudgetExceeded { limit: u64 },
    #[error("SHACL path depth budget {limit} was exhausted")]
    PathDepthExceeded { limit: usize },
    #[error("cyclic SHACL shape evaluation at shape `{shape}` and focus `{focus}`")]
    CyclicShapeEvaluation { shape: String, focus: String },
    #[error("SHACL validation result limit {limit} was exceeded")]
    ResultLimitExceeded { limit: usize },
    #[error("invalid sh:pattern `{pattern}` with flags `{flags}` on shape `{shape}`: {message}")]
    InvalidPattern {
        shape: String,
        pattern: String,
        flags: String,
        message: String,
    },
}
