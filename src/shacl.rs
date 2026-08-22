//! Craqle-owned SHACL compilation types.

use std::sync::Arc;
use std::time::Duration;

use crate::shacl_impl::model::CompiledSchemaInner;
use crate::{CrateViolation, EncodedTerm, QueryCancellation, ReadStatistics, RoCrateVersion};

/// SHACL feature profile implemented by Craqle's native validator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ShaclProfile {
    /// Craqle SHACL Core Subset v1.
    ///
    /// Supports node, class, subjects-of, objects-of, and implicit-class
    /// targets; direct, inverse, sequence, alternative, zero-or-one,
    /// zero-or-more, and one-or-more paths; and the native constraint forms
    /// documented in the README. Recursive shapes, SHACL-SPARQL, SHACL-JS,
    /// SHACL-AF, custom components and targets, reifier shapes, RDF-star, and
    /// remote imports are unsupported.
    CoreSubsetV1,
}

/// Persisted model version used by the native SHACL compiler.
pub const SHACL_COMPILER_MODEL_VERSION: u32 = 2;

/// Startup policy for durable SHACL work left pending by an earlier process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingReplayPolicy {
    ReplayAllBeforeOpen,
    ReplayBounded {
        max_graphs: usize,
        max_elapsed: Duration,
    },
    Defer,
}

impl Default for PendingReplayPolicy {
    fn default() -> Self {
        Self::ReplayBounded {
            max_graphs: 100,
            max_elapsed: Duration::from_millis(250),
        }
    }
}

/// Work performed while repairing or replaying the durable SHACL queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingReplayStatistics {
    pub binding_records_scanned: u64,
    pub pending_queue_entries_scanned: u64,
    pub graphs_settled: u64,
    pub reports_produced: u64,
    pub elapsed: Duration,
}

/// One queued graph whose settlement failed without changing its source write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingReplayFailure {
    pub graph: crate::GraphId,
    pub bindings: Vec<ShaclBinding>,
    pub data_version: Option<[u8; 32]>,
    pub error: String,
}

/// Complete outcome of one bounded pending replay pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingReplayOutcome {
    pub statistics: PendingReplayStatistics,
    pub failures: Vec<PendingReplayFailure>,
    pub budget_exhausted: bool,
}

/// Cheap durable queue state. Reading it never validates a graph.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PendingShaclQueueStatus {
    pub pending_count: u64,
    pub settlement_failures: u64,
}

/// Cumulative SHACL work and lock counters for one open store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShaclRuntimeStatistics {
    pub binding_lock_wait_ns: u64,
    pub binding_lock_hold_ns: u64,
    pub graph_commit_lock_wait_ns: u64,
    pub validation_ns: u64,
    pub settlement_ns: u64,
    pub settlement_failures: u64,
    pub status_bindings_read: u64,
    pub status_version_checks: u64,
    pub status_shape_compilations: u64,
    pub status_full_shape_scans: u64,
}

/// Execution path requested for a complete native SHACL validation.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ShaclEvaluationMode {
    #[default]
    Auto,
    Delta,
    Full,
}

/// Policy applied to local writes for one data/shapes graph binding.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum ShaclWritePolicy {
    Enforce,
    Advisory,
    #[default]
    Disabled,
}

/// Minimum result severity that rejects an enforcing write.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum ShaclBlockingSeverity {
    AnyResult,
    WarningOrViolation,
    #[default]
    ViolationOnly,
}

/// Persistable limits and compiler options for one SHACL binding.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShaclBindingOptions {
    pub rocrate_version: RoCrateVersion,
    pub allow_local_imports: bool,
    pub max_results: usize,
    pub max_path_edges: u64,
    pub max_path_depth: usize,
    pub blocking_severity: ShaclBlockingSeverity,
}

impl Default for ShaclBindingOptions {
    fn default() -> Self {
        let validation = ShaclValidationOptions::default();
        Self {
            rocrate_version: RoCrateVersion::default(),
            allow_local_imports: false,
            max_results: validation.max_results,
            max_path_edges: validation.max_path_edges,
            max_path_depth: validation.max_path_depth,
            blocking_severity: validation.blocking_severity,
        }
    }
}

impl ShaclBindingOptions {
    pub(crate) fn compile_options(&self) -> ShaclCompileOptions {
        ShaclCompileOptions {
            rocrate_version: self.rocrate_version,
            allow_local_imports: self.allow_local_imports,
        }
    }

    pub(crate) fn validation_options(&self) -> ShaclValidationOptions {
        ShaclValidationOptions {
            cancellation: QueryCancellation::new(),
            max_results: self.max_results,
            max_path_edges: self.max_path_edges,
            max_path_depth: self.max_path_depth,
            execution_mode: ShaclEvaluationMode::Auto,
            blocking_severity: self.blocking_severity,
        }
    }
}

/// A persisted association between one data graph and one shapes graph.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShaclBinding {
    pub data_graph: crate::GraphId,
    pub shapes_graph: crate::GraphId,
    pub policy: ShaclWritePolicy,
    pub validation_options: ShaclBindingOptions,
}

/// Latest known advisory state for a persisted binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum ShaclValidationState {
    Pending,
    Valid,
    Invalid,
    Failed,
}

/// Persisted complete report, or an explicit pending/failed state.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShaclBindingStatus {
    pub binding: ShaclBinding,
    pub state: ShaclValidationState,
    pub report: Option<ShaclValidationReport>,
    pub error: Option<String>,
    pub data_version: [u8; 32],
    pub shapes_version: [u8; 32],
    pub schema_fingerprint: [u8; 32],
    pub compiler_model_version: u32,
    pub(crate) shape_versions: Vec<(crate::GraphId, [u8; 32])>,
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
    pub execution_mode: ShaclEvaluationMode,
    pub blocking_severity: ShaclBlockingSeverity,
}

impl Default for ShaclValidationOptions {
    fn default() -> Self {
        Self {
            cancellation: QueryCancellation::new(),
            max_results: 10_000,
            max_path_edges: 1_000_000,
            max_path_depth: 128,
            execution_mode: ShaclEvaluationMode::Auto,
            blocking_severity: ShaclBlockingSeverity::ViolationOnly,
        }
    }
}

/// Work performed by one native SHACL validation.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShaclValidationStatistics {
    /// Concrete path that completed this validation.
    pub selected_mode: ShaclEvaluationMode,
    pub estimated_delta_work: u64,
    pub estimated_full_work: u64,
    pub estimated_affected_shapes: u64,
    pub estimated_focus_nodes: u64,
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
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct ShaclMessage {
    pub language: Option<String>,
    pub text: String,
}

/// One deterministic native SHACL validation result.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShaclValidationReport {
    pub conforms: bool,
    pub accepted_by_write_policy: bool,
    pub results: Vec<ShaclValidationResult>,
    pub statistics: ShaclValidationStatistics,
}

impl ShaclValidationReport {
    pub(crate) fn refresh_outcomes(&mut self, threshold: ShaclBlockingSeverity) {
        self.conforms = self.results.is_empty();
        self.accepted_by_write_policy = !self
            .results
            .iter()
            .any(|result| severity_blocks(&result.severity, threshold));
    }

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

fn severity_blocks(severity: &EncodedTerm, threshold: ShaclBlockingSeverity) -> bool {
    let rank = match severity.0.as_str() {
        concat!("<", "http://www.w3.org/ns/shacl#", "Trace>") => Some(0),
        concat!("<", "http://www.w3.org/ns/shacl#", "Debug>") => Some(1),
        concat!("<", "http://www.w3.org/ns/shacl#", "Info>") => Some(2),
        concat!("<", "http://www.w3.org/ns/shacl#", "Warning>") => Some(3),
        concat!("<", "http://www.w3.org/ns/shacl#", "Violation>") => Some(4),
        _ => None,
    };
    let minimum = match threshold {
        ShaclBlockingSeverity::AnyResult => 0,
        ShaclBlockingSeverity::WarningOrViolation => 3,
        ShaclBlockingSeverity::ViolationOnly => 4,
    };
    rank.is_none_or(|rank| rank >= minimum)
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
        ShaclProfile::CoreSubsetV1
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
    #[error("recursive SHACL shape `{shape}` is unsupported")]
    UnsupportedRecursiveShape { shape: String },
    #[error("RDF-star term is unsupported in Craqle SHACL Core Subset v1: {term}")]
    UnsupportedRdfStarTerm { term: String },
    #[error("SHACL data graph `{graph}` does not exist")]
    DataGraphNotFound { graph: String },
    #[error("SHACL shapes graph `{graph}` changed repeatedly during validation")]
    SchemaChangedDuringValidation { graph: String },
    #[error("a graph cannot be validated against shapes changed by the same write: `{graph}`")]
    ShapesGraphMutationUnsupported { graph: String },
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
    #[error("SHACL delta validation cannot run: {reason}")]
    DeltaExecutionUnavailable { reason: String },
    #[error("invalid sh:pattern `{pattern}` with flags `{flags}` on shape `{shape}`: {message}")]
    InvalidPattern {
        shape: String,
        pattern: String,
        flags: String,
        message: String,
    },
}

impl ShaclError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::UnsupportedComponent { .. }
            | Self::ImportsDisabled { .. }
            | Self::ImportNotLocal { .. }
            | Self::UnsupportedRecursiveShape { .. }
            | Self::UnsupportedRdfStarTerm { .. }
            | Self::ShapesGraphMutationUnsupported { .. }
            | Self::DeltaExecutionUnavailable { .. } => crate::CraqleErrorKind::Unsupported,
            Self::SchemaChangedDuringValidation { .. } => {
                crate::CraqleErrorKind::StalePreparedState
            }
            Self::ValidationCancelled => crate::CraqleErrorKind::Cancelled,
            Self::PathBudgetExceeded { .. }
            | Self::PathDepthExceeded { .. }
            | Self::ResultLimitExceeded { .. } => crate::CraqleErrorKind::ValidationLimit,
            Self::ShapesGraphNotFound { .. }
            | Self::IllFormedShapes { .. }
            | Self::ImportCycle { .. }
            | Self::DataGraphNotFound { .. }
            | Self::CyclicShapeEvaluation { .. }
            | Self::InvalidPattern { .. } => crate::CraqleErrorKind::InvalidInput,
        }
    }
}
