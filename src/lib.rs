//! Craqle stores, validates, queries, searches, and replicates RO-Crates.
//!
//! The integration surface is the root API: [`CraqleNode`], the typed request
//! structs, and RO-Crate JSON-LD import/export. Everything under
//! `src/internal/` is private to the crate.
//!
//! # Compatibility in 0.2.x
//!
//! Documented public APIs remain source compatible throughout 0.2.x unless a
//! correctness or security defect makes that impossible. Authoritative CRDT
//! data written by 0.2 remains readable by later 0.2 releases. Query and search
//! indexes and compiled SHACL caches are derived data and may be rebuilt or
//! discarded. Unsupported forms return an error. A future 0.3 release may make
//! breaking changes with a migration note.

#![warn(unreachable_pub)]

#[path = "internal/cache.rs"]
mod cache;
#[path = "internal/core.rs"]
mod core;
#[path = "internal/count_exec.rs"]
mod count_exec;
#[path = "internal/count_plan.rs"]
mod count_plan;
#[path = "internal/planner.rs"]
mod planner;
#[allow(dead_code)]
#[path = "internal/query_context.rs"]
mod query_context;
#[allow(dead_code)]
#[path = "internal/query_cursor.rs"]
mod query_cursor;
#[path = "internal/query_worker.rs"]
mod query_worker;
#[allow(dead_code)]
#[path = "internal/rdf_read.rs"]
mod rdf_read;
#[path = "internal/replication.rs"]
mod replication;
#[path = "internal/rocrate.rs"]
mod rocrate;
#[path = "internal/rules.rs"]
mod rules;
#[cfg(feature = "search")]
#[path = "internal/search.rs"]
mod search;
#[cfg(not(feature = "search"))]
#[path = "search_stub.rs"]
mod search;
#[path = "internal/search_queue.rs"]
mod search_queue;
#[cfg(feature = "shacl-core")]
#[path = "internal/shacl/mod.rs"]
mod shacl_impl;
#[path = "internal/sparql.rs"]
mod sparql;
#[path = "internal/sparql_fast_path.rs"]
mod sparql_fast_path;
#[path = "internal/store.rs"]
mod store;
#[path = "internal/validation_delta.rs"]
mod validation_delta;

mod auth;
#[cfg(feature = "shacl-core")]
pub mod shacl;
mod sync;

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::panic;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
#[cfg(feature = "shacl-core")]
use std::time::Instant;

use crate::core::{
    EncodedTerm as CoreEncodedTerm, MaterializedQuadChange as CoreMaterializedQuadChange,
};
#[cfg(feature = "shacl-core")]
use crate::query_context::ReadContext;
#[cfg(feature = "shacl-core")]
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::replication::ReplicationEngine;
use crate::rocrate::RoCrateManager;
use crate::search::SearchIndex;
#[cfg(feature = "shacl-core")]
use crate::shacl_impl::ShaclCompiler;
use crate::sparql::SparqlEngine;
use crate::store::GraphStore;
#[cfg(feature = "shacl-core")]
use crate::store::hash_term;
use chrono::Utc;
use oxrdf::{NamedNode, Term};

pub use crate::core::{
    ActorId, Batch, CrateViolation, EncodedTerm, EventId, GraphDiagnostics, GraphId, GraphPolicy,
    GraphTombstone, MaterializedQuadChange, PolicyTag, PredicateFilter, TaggedGraphPolicy,
    UnsupportedRdfStarTerm, VectorClock, vocab,
};
pub use crate::core::{Dot, GraphReplicaSnapshot, QuadOp, SnapshotQuadState};
pub use crate::planner::{JoinKind, JoinMode, PlannedJoin};
pub use crate::query_context::{QueryCancellation, QueryReadMode, ReadAccessPath, ReadStatistics};
pub use crate::replication::{CheckMode, DiagnosticsMode, MergeError, UpdateError, WriteChecks};
pub use crate::rocrate::{
    AppendDataEntitiesReport, CanonicalJsonLd, NewDataEntity, PrepareRoCrateOptions,
    PreparedGraphBase, PreparedRoCrateDocument, PreparedRoCrateStatistics, RoCrateError,
    RoCrateImportLimits, RoCratePage, canonicalize_jsonld, validate_rocrate_jsonld,
};
pub use crate::search::SearchHit;
#[cfg(feature = "shacl-core")]
pub use crate::shacl::{
    CompiledShaclSchema, PendingReplayFailure, PendingReplayOutcome, PendingReplayPolicy,
    PendingReplayStatistics, PendingShaclQueueStatus, SHACL_COMPILER_MODEL_VERSION, ShaclBinding,
    ShaclBindingOptions, ShaclBindingStatus, ShaclBlockingSeverity, ShaclCompileOptions,
    ShaclCompileStatistics, ShaclError, ShaclEvaluationMode, ShaclMessage, ShaclProfile,
    ShaclRuntimeStatistics, ShaclValidationOptions, ShaclValidationReport, ShaclValidationResult,
    ShaclValidationState, ShaclValidationStatistics, ShaclWritePolicy,
};
pub use crate::sparql::{
    PreparedQuery, QueryExecution, QueryExecutionStatistics, QueryLimits, QueryLogicalOperator,
    QueryOptions, QueryPhysicalOperator, QueryPlan, QueryPlanNode, QueryResults, UpdateLimits,
    UpdateOptions,
};
pub use crate::sparql_fast_path::{QueryFastPathKind, QueryFastPathMode};
pub use crate::sync::{
    CraqleGraphEvent, CraqleIrokleOptions, CraqleSyncError, DenyRemotePolicyChanges,
    IrokleGraphSync, RejectedReplicationRecord, RemotePolicyAuthorizer, TopicCursorRepairAudit,
    topic_cursor_digest,
};
pub use auth::{
    Action, AllowAllAuthorizer, AuthorizationError, Authorizer, DenyAllAuthorizer, GrantAuthorizer,
    PermissionGrant, PermissionLevel,
};
pub use irokle;

/// Stable high-level classification for public Craqle failures.
///
/// Detailed error variants may gain additional context during 0.2.x. Callers
/// that need durable control flow should use [`CraqleError::kind`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[non_exhaustive]
pub enum CraqleErrorKind {
    InvalidInput,
    Unsupported,
    Unauthorized,
    Conflict,
    StalePreparedState,
    QueryLimit,
    ValidationLimit,
    Storage,
    CorruptDerivedData,
    CorruptAuthoritativeData,
    DependencyUnavailable,
    Cancelled,
}

/// Authoritative on-disk format understood by this release.
///
/// The version covers CRDT source state, graph recovery metadata, and committed
/// policy bindings. Disposable indexes and caches have their own format
/// markers and do not change this version.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct DiskFormatVersion {
    pub major: u16,
    pub minor: u16,
}

impl DiskFormatVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

/// Current authoritative disk format written by Craqle 0.2.
pub const DISK_FORMAT_VERSION: DiskFormatVersion = DiskFormatVersion::new(1, 0);

/// Test-only stall between a publish and its own apply, in microseconds.
///
/// The pair is one critical section: the window between the two is exactly
/// where a concurrent write slips in and makes apply order differ from publish
/// order. In a real run that window is a few instructions wide, far too narrow
/// to hit on purpose, so tests widen it. Compiled out of every non-test build.
#[cfg(test)]
static PUBLISH_APPLY_STALL_MICROS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn stall_publish_apply() {
    let micros = PUBLISH_APPLY_STALL_MICROS.load(Ordering::Relaxed);
    if micros == 0 {
        return;
    }
    // Jittered: a fixed stall would delay every writer equally and so preserve
    // the order they entered in, which is the order under test.
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| u64::from(since.subsec_nanos()));
    std::thread::sleep(Duration::from_micros(jitter % micros + 1));
}

#[cfg(not(test))]
fn stall_publish_apply() {}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CraqleError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("authorization: {0}")]
    Authorization(#[from] AuthorizationError),
    #[error("store: {0}")]
    Store(#[from] store::StoreError),
    #[error("search: {0}")]
    Search(#[from] search::SearchError),
    #[error("sparql: {0}")]
    Sparql(sparql::SparqlError),
    #[error("SPARQL query cancelled")]
    QueryCancelled,
    #[error("update: {0}")]
    Update(#[from] replication::UpdateError),
    #[error("merge: {0}")]
    Merge(#[from] replication::MergeError),
    #[error("rocrate: {0}")]
    RoCrate(#[from] rocrate::RoCrateError),
    #[cfg(feature = "shacl-core")]
    #[error("shacl: {0}")]
    Shacl(#[from] ShaclError),
    #[cfg(feature = "shacl-core")]
    #[error("prepared RO-Crate document did not conform to the requested policy")]
    RoCratePolicyRejected(Box<RoCratePolicyReport>),
    #[cfg(feature = "shacl-core")]
    #[error("prepared commit mode {mode:?} requires a compiled RO-Crate policy")]
    RoCratePolicyRequired { mode: PreparedCommitMode },
    #[error("sync input rejected: {0}")]
    SyncInputRejected(String),
    #[error("sync: {0}")]
    Sync(#[from] sync::CraqleSyncError),
    #[error("search worker: {0}")]
    SearchWorker(String),
    #[error("unsupported update across multiple graphs")]
    MultiGraphUpdateUnsupported,
    #[error(transparent)]
    UnsupportedRdfStarTerm(#[from] UnsupportedRdfStarTerm),
    #[error("replication record rejected: {reason}")]
    ReplicationRejected {
        error_kind: CraqleErrorKind,
        reason: String,
    },
}

impl From<sparql::SparqlError> for CraqleError {
    fn from(error: sparql::SparqlError) -> Self {
        match error {
            sparql::SparqlError::Cancelled => Self::QueryCancelled,
            sparql::SparqlError::Store(store::StoreError::Cancelled) => Self::QueryCancelled,
            sparql::SparqlError::Authorization(error) => Self::Authorization(error),
            error => Self::Sparql(error),
        }
    }
}

impl CraqleError {
    /// Stable category for programmatic error handling in the 0.2 series.
    pub fn kind(&self) -> CraqleErrorKind {
        match self {
            Self::Io(_) | Self::SearchWorker(_) => CraqleErrorKind::Storage,
            Self::Authorization(_) => CraqleErrorKind::Unauthorized,
            Self::Store(error) => error.kind(),
            Self::Search(error) => error.kind(),
            Self::Sparql(error) => error.kind(),
            Self::QueryCancelled => CraqleErrorKind::Cancelled,
            Self::Update(error) => error.kind(),
            Self::Merge(error) => error.kind(),
            Self::RoCrate(error) => error.kind(),
            #[cfg(feature = "shacl-core")]
            Self::Shacl(error) => error.kind(),
            #[cfg(feature = "shacl-core")]
            Self::RoCratePolicyRejected(_) => CraqleErrorKind::InvalidInput,
            #[cfg(feature = "shacl-core")]
            Self::RoCratePolicyRequired { .. } => CraqleErrorKind::InvalidInput,
            Self::SyncInputRejected(_) => CraqleErrorKind::InvalidInput,
            Self::Sync(error) => error.kind(),
            Self::MultiGraphUpdateUnsupported => CraqleErrorKind::Unsupported,
            Self::UnsupportedRdfStarTerm(_) => CraqleErrorKind::Unsupported,
            Self::ReplicationRejected { error_kind, .. } => *error_kind,
        }
    }

    /// Whether this rejects a record for what it contains, so a reconcile may
    /// quarantine it; everything else is retryable and must stall instead.
    pub fn rejects_record(&self) -> bool {
        match self {
            Self::Merge(MergeError::InputRejected(_)) | Self::SyncInputRejected(_) => true,
            Self::ReplicationRejected { .. } => true,
            Self::Merge(MergeError::Store(error)) | Self::Store(error) => error.rejects_record(),
            Self::Sync(error) => error.rejects_record(),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, CraqleError>;

/// Request-path durability policy for callers with an external durable WAL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CraqleRequestDurability {
    /// Persist Craqle's Fjall graph store before returning.
    #[default]
    Durable,
    /// Apply locally but let the caller's already-durable WAL drive recovery.
    WalAlreadyDurable,
}

impl CraqleRequestDurability {
    fn persists_fjall(self) -> bool {
        matches!(self, Self::Durable)
    }

    fn publishes_irokle(self) -> bool {
        matches!(self, Self::Durable)
    }
}

/// Fjall persistence mode used when Craqle explicitly persists its graph store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CraqleFjallPersistMode {
    /// Flush to Fjall's configured buffer without forcing an OS sync.
    #[default]
    Buffer,
    /// Sync file data but not necessarily metadata.
    SyncData,
    /// Sync file data and metadata before returning.
    SyncAll,
}

impl CraqleFjallPersistMode {
    fn into_store_mode(self) -> fjall::PersistMode {
        match self {
            Self::Buffer => fjall::PersistMode::Buffer,
            Self::SyncData => fjall::PersistMode::SyncData,
            Self::SyncAll => fjall::PersistMode::SyncAll,
        }
    }

    fn from_store_mode(mode: fjall::PersistMode) -> Self {
        match mode {
            fjall::PersistMode::Buffer => Self::Buffer,
            fjall::PersistMode::SyncData => Self::SyncData,
            fjall::PersistMode::SyncAll => Self::SyncAll,
        }
    }
}

/// Lifecycle state of Craqle's disposable persistent query indexes.
///
/// The canonical CRDT quad state remains authoritative in every state. A
/// non-ready state affects query-index availability only, never source reads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum QueryIndexState {
    Missing,
    Building,
    Ready,
    Failed(String),
}

/// Persisted-query-index lifecycle and row-count summary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct QueryIndexStatus {
    pub schema_version: u32,
    pub state: QueryIndexState,
    /// Generation fencing dense query IDs and any physical plan that embeds them.
    pub query_id_generation: u64,
    /// Dense IDs allocated in this generation, including retained IDs for deleted rows.
    pub query_term_ids: u64,
    /// Live canonical source rows in the coherent status snapshot.
    pub source_live_quads: u64,
    /// Indexed live rows in the same coherent snapshot.
    pub indexed_quads: u64,
    pub last_build_sequence: u64,
}

/// A bounded diagnostic report for persistent-query-index verification.
///
/// Problems are stable implementation identifiers only; no RDF term or value
/// bytes are included.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct QueryIndexVerification {
    pub full: bool,
    pub valid: bool,
    pub source_live_quads: u64,
    pub indexed_quads: u64,
    pub checked_source_rows: u64,
    pub checked_index_rows: u64,
    pub problems: Vec<String>,
}

/// Amount of persistent-query-index inspection requested by an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum QueryIndexVerificationMode {
    Sample,
    Full,
}

impl From<bool> for QueryIndexVerificationMode {
    fn from(full: bool) -> Self {
        if full { Self::Full } else { Self::Sample }
    }
}

/// A supported RO-Crate specification version.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum RoCrateVersion {
    V1_1,
    V1_2,
    #[default]
    V1_3,
}

impl RoCrateVersion {
    pub(crate) const fn context_url(self) -> &'static str {
        match self {
            Self::V1_1 => "https://w3id.org/ro/crate/1.1/context",
            Self::V1_2 => "https://w3id.org/ro/crate/1.2/context",
            Self::V1_3 => "https://w3id.org/ro/crate/1.3/context",
        }
    }

    pub(crate) const fn specification_url(self) -> &'static str {
        match self {
            Self::V1_1 => "https://w3id.org/ro/crate/1.1",
            Self::V1_2 => "https://w3id.org/ro/crate/1.2",
            Self::V1_3 => "https://w3id.org/ro/crate/1.3",
        }
    }

    pub(crate) const fn context_bytes(self) -> &'static [u8] {
        match self {
            Self::V1_1 => include_bytes!("resources/ro_crate_1_1.jsonld"),
            Self::V1_2 => include_bytes!("resources/ro_crate_1_2.jsonld"),
            Self::V1_3 => include_bytes!("resources/ro_crate_1_3.jsonld"),
        }
    }
}

/// Stable identifier for one compiled RO-Crate SHACL policy.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PolicyId([u8; 32]);

#[cfg(feature = "shacl-core")]
impl PolicyId {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Compiled native policy and every shapes-version fence it depends on.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Debug)]
pub struct CompiledRoCratePolicy {
    pub policy_id: PolicyId,
    pub shacl: CompiledShaclSchema,
    pub compiler_model_version: u32,
    pub root_shapes_graph: GraphId,
    pub root_shapes_version: [u8; 32],
    pub imported_shapes: Vec<(GraphId, [u8; 32])>,
}

/// Limits used while evaluating a prepared raw RO-Crate candidate.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Debug, Default)]
pub struct RoCratePolicyOptions {
    pub validation: ShaclValidationOptions,
}

/// Parse-once preparation and native-validation work for one policy report.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RoCratePolicyStatistics {
    pub parse_count: u64,
    pub parse_time: Duration,
    pub encode_time: Duration,
    pub structural_time: Duration,
    pub diff_time: Duration,
    pub target_time: Duration,
    pub constraint_time: Duration,
    pub report_time: Duration,
    pub encoded_triples: u64,
    pub encoded_changes: u64,
}

/// One complete structural and native-SHACL policy result for a raw document.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoCratePolicyReport {
    pub conforms: bool,
    pub accepted_by_write_policy: bool,
    pub detected_version: RoCrateVersion,
    pub document_digest: [u8; 32],
    pub rocrate_violations: Vec<CrateViolation>,
    pub shacl: ShaclValidationReport,
    pub statistics: RoCratePolicyStatistics,
}

/// Policy behavior requested when committing a prepared document.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PreparedCommitMode {
    Enforce,
    Advisory,
    StructuralOnly,
}

/// Source batch and optional advisory/enforcement report from a prepared commit.
#[cfg(feature = "shacl-core")]
#[derive(Clone, Debug)]
pub struct PreparedRoCrateCommitOutcome {
    pub batch: Batch,
    pub policy_report: Option<RoCratePolicyReport>,
}

/// Input for creating a new RO-Crate graph.
#[derive(Debug, Clone)]
pub struct CreateCrateRequest {
    pub graph: GraphId,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: Option<String>,
    pub policy: GraphPolicy,
}

#[derive(Debug, Clone, Default)]
pub struct CreateCrateOptions {
    pub version: RoCrateVersion,
    pub license: Option<String>,
}

impl CreateCrateRequest {
    pub fn new(
        graph: GraphId,
        name: impl Into<String>,
        description: impl Into<String>,
        date_published: impl Into<String>,
        license: Option<String>,
        policy: GraphPolicy,
    ) -> Self {
        Self {
            graph,
            name: name.into(),
            description: description.into(),
            date_published: date_published.into(),
            license,
            policy,
        }
    }
}

/// Input for creating or replacing a single RO-Crate entity.
#[derive(Debug, Clone)]
pub struct CreateEntityRequest {
    pub graph: GraphId,
    pub entity_id: String,
    pub entity_type: String,
    pub name: String,
    pub additional_triples: Vec<(NamedNode, Term)>,
}

/// Input for patching the properties present on a single RO-Crate entity.
#[derive(Debug, Clone)]
pub struct PatchEntityRequest {
    pub entity: CreateEntityRequest,
    /// Predicates explicitly present in the patch, including empty values.
    pub replaced_predicates: Vec<NamedNode>,
}

/// Search hit together with hydrated RDF properties.
#[derive(Debug, Clone)]
pub struct HydratedSearchHit {
    pub hit: SearchHit,
    pub properties: Vec<(EncodedTerm, EncodedTerm)>,
}

/// Full-text search over every graph the caller may read.
pub struct SearchRequest<'a> {
    pub query: &'a str,
    pub limit: usize,
}

/// Full-text search restricted to an explicit set of graphs.
pub struct GraphSearchRequest<'a> {
    pub graphs: &'a [GraphId],
    pub query: &'a str,
    pub limit: usize,
}

/// One subject to resolve into its visible `(predicate, object)` pairs.
pub struct DescribeRequest<'a> {
    pub graph: &'a GraphId,
    pub subject_id: &'a str,
}

/// Hard cap on a caller-supplied search limit, applied at every entry point.
///
/// Tantivy's top-k collector pre-allocates `limit * 2` and the over-fetch
/// multiplies the limit again before that, so an unbounded limit is an
/// allocation the caller picks — `fts:limit 10000000000000` from a remote
/// query aborted the process. Ten thousand rows is well past any real page
/// and still a trivially sized collector.
pub const MAX_SEARCH_LIMIT: usize = 10_000;

#[cfg(test)]
const MAX_SYNC_POLICY_PATHS: usize = 1_024;
const SEARCH_QUEUE_FLUSH_CHUNK: usize = 50_000;
/// Smallest Tantivy over-fetch before authorization filtering.
const SEARCH_MIN_FETCH: usize = 64;
/// Above this many selected graphs, `search_graphs` runs one filtered search
/// instead of one full top-k collection per graph.
const SEARCH_GRAPHS_PER_GRAPH_LIMIT: usize = 8;
/// Graphs reindexed between Tantivy commits in `reindex_search`.
const REINDEX_COMMIT_BATCH_GRAPHS: usize = 64;

enum SearchWorkerMessage {
    Wake,
    Flush(mpsc::Sender<std::result::Result<(), String>>),
    Stop,
}

struct SearchUpdateWorker {
    sender: mpsc::Sender<SearchWorkerMessage>,
    /// `true` once a wake has been sent and not yet consumed by the worker.
    /// Collapses a burst of writes into a single channel message instead of
    /// one unbounded-channel send per write.
    wake_pending: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SearchUpdateWorker {
    fn start(store: Arc<GraphStore>, search: Arc<SearchIndex>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let wake_pending = Arc::new(AtomicBool::new(false));
        let ctx = SearchWorkerCtx {
            store,
            search,
            wake_pending: wake_pending.clone(),
        };
        let handle = std::thread::spawn(move || {
            run_search_update_worker(receiver, ctx);
        });

        Self {
            sender,
            wake_pending,
            handle: Some(handle),
        }
    }

    /// Ask the worker to drain the FTS queues.
    ///
    /// Skipping the send while a wake is already outstanding is safe: the
    /// worker clears the flag *before* it starts draining, so any enqueue that
    /// observed the flag set is guaranteed to be visible to that drain. A
    /// one-second receive timeout backstops the flag either way.
    fn wake(&self) {
        if self.wake_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        if self.sender.send(SearchWorkerMessage::Wake).is_err() {
            self.wake_pending.store(false, Ordering::SeqCst);
        }
    }

    fn flush(&self) -> Result<()> {
        let (sender, receiver) = mpsc::channel();
        self.sender
            .send(SearchWorkerMessage::Flush(sender))
            .map_err(|_| CraqleError::SearchWorker("stopped".to_string()))?;
        receiver
            .recv()
            .map_err(|_| CraqleError::SearchWorker("stopped".to_string()))?
            .map_err(CraqleError::SearchWorker)
    }
}

impl Drop for SearchUpdateWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(SearchWorkerMessage::Stop);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Everything the background indexer thread owns.
struct SearchWorkerCtx {
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
    wake_pending: Arc<AtomicBool>,
}

fn run_search_update_worker(receiver: mpsc::Receiver<SearchWorkerMessage>, ctx: SearchWorkerCtx) {
    loop {
        let mut flush_replies = Vec::new();
        if collect_search_worker_messages(&receiver, &mut flush_replies) {
            for reply in flush_replies {
                let _ = reply.send(Err("stopped".to_string()));
            }
            break;
        }

        // Cleared before the drain, so a writer that enqueues after this point
        // always gets a fresh wake through.
        ctx.wake_pending.store(false, Ordering::SeqCst);

        let result = drain_search_queue_guarded(&ctx);
        let failed = result.is_err();
        for reply in flush_replies {
            let _ = reply.send(result.clone());
        }
        if failed {
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

fn collect_search_worker_messages(
    receiver: &mpsc::Receiver<SearchWorkerMessage>,
    flush_replies: &mut Vec<mpsc::Sender<std::result::Result<(), String>>>,
) -> bool {
    match receiver.recv_timeout(Duration::from_secs(1)) {
        Ok(SearchWorkerMessage::Wake) | Err(mpsc::RecvTimeoutError::Timeout) => {}
        Ok(SearchWorkerMessage::Flush(reply)) => flush_replies.push(reply),
        Ok(SearchWorkerMessage::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => return true,
    }

    while let Ok(message) = receiver.try_recv() {
        match message {
            SearchWorkerMessage::Wake => {}
            SearchWorkerMessage::Flush(reply) => flush_replies.push(reply),
            SearchWorkerMessage::Stop => return true,
        }
    }

    false
}

/// Runs one drain cycle, turning a panic into an error rather than losing the
/// indexer thread — it is the only thread that can repair the index.
fn drain_search_queue_guarded(ctx: &SearchWorkerCtx) -> std::result::Result<(), String> {
    let drain = panic::AssertUnwindSafe(|| flush_search_queue(&ctx.store, &ctx.search));
    match panic::catch_unwind(drain) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(payload) => Err(format!(
            "search worker panicked: {}",
            panic_message(&*payload)
        )),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Drains the FTS queues until everything enqueued *before this call* is indexed.
///
/// Bounded by the dirty token observed on entry: without that, a writer that
/// keeps enqueueing holds the loop open and `flush_search_updates()` never
/// returns.
fn flush_search_queue(store: &GraphStore, search: &SearchIndex) -> Result<()> {
    #[cfg(test)]
    if search.take_armed_drain_panic() {
        panic!("injected drain panic");
    }

    let max_token = store.current_dirty_token();
    let mut processed_any = false;
    loop {
        let bound = search::QueueBound {
            chunk: SEARCH_QUEUE_FLUSH_CHUNK,
            max_token: Some(max_token),
        };
        let processed = search.process_queued_updates(store, bound)?;
        if processed == 0 {
            if processed_any {
                store.persist()?;
            }
            return Ok(());
        }
        processed_any = true;
    }
}

/// A graph whose reindex scan was pinned at `upto`. Only queue entries at or
/// below that token were covered by the scan.
struct ScannedGraph {
    graph: GraphId,
    upto: u64,
}

/// Main application handle for local RO-Crate operations: authorization-aware
/// crate writes, JSON-LD export, search, and replication.
pub struct CraqleNode {
    actor: ActorId,
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
    search_worker: SearchUpdateWorker,
    sparql: Arc<SparqlEngine>,
    #[cfg(feature = "shacl-core")]
    shacl: Arc<ShaclCompiler>,
    #[cfg(feature = "shacl-core")]
    startup_pending_replay: PendingReplayOutcome,
    replication: Arc<ReplicationEngine>,
    local_replication: Arc<ReplicationEngine>,
    sync: Option<Arc<dyn sync::CraqleGraphSync>>,
    remote_policy_authorizer: Arc<dyn RemotePolicyAuthorizer>,
    reconcile_guard: Mutex<()>,
    replication_rejections: AtomicU64,
    /// Set by a test to hold a reindex between a graph's scan and the queue
    /// clear that covers it.
    #[cfg(test)]
    reindex_gate: std::sync::Mutex<Option<ReindexGate>>,
}

/// Reports that a reindex reached the point between a scan and its clear, then
/// waits to be released. Test-only.
#[cfg(test)]
struct ReindexGate {
    reached: mpsc::Sender<()>,
    go: mpsc::Receiver<()>,
}

/// Configuration used when constructing a [`CraqleNode`].
pub struct CraqleOptions {
    actor: ActorId,
    sync: Option<Arc<dyn sync::CraqleGraphSync>>,
    remote_policy_authorizer: Arc<dyn RemotePolicyAuthorizer>,
    search_storage: SearchStorage,
    graph_store_persist_mode: CraqleFjallPersistMode,
    #[cfg(feature = "shacl-core")]
    pending_replay_policy: PendingReplayPolicy,
}

/// Storage backend used for the full-text search index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum SearchStorage {
    #[default]
    Disk,
    Memory,
}

impl Default for CraqleOptions {
    fn default() -> Self {
        Self {
            actor: ActorId::random(),
            sync: None,
            remote_policy_authorizer: Arc::new(DenyRemotePolicyChanges),
            search_storage: SearchStorage::default(),
            graph_store_persist_mode: CraqleFjallPersistMode::default(),
            #[cfg(feature = "shacl-core")]
            pending_replay_policy: PendingReplayPolicy::default(),
        }
    }
}

impl CraqleOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.actor = actor;
        self
    }

    pub fn with_search_storage(mut self, search_storage: SearchStorage) -> Self {
        self.search_storage = search_storage;
        self
    }

    pub fn with_graph_store_persist_mode(mut self, mode: CraqleFjallPersistMode) -> Self {
        self.graph_store_persist_mode = mode;
        self
    }

    pub fn graph_store_persist_mode(&self) -> CraqleFjallPersistMode {
        self.graph_store_persist_mode
    }

    #[cfg(feature = "shacl-core")]
    pub fn with_pending_replay_policy(mut self, policy: PendingReplayPolicy) -> Self {
        self.pending_replay_policy = policy;
        self
    }

    pub fn with_irokle<S: irokle::Storage>(
        mut self,
        node: irokle::Irokle<S>,
        options: CraqleIrokleOptions,
    ) -> Self {
        self.sync = Some(Arc::new(IrokleGraphSync::new(node, options)));
        self
    }

    pub fn with_remote_policy_authorizer(
        mut self,
        authorizer: Arc<dyn RemotePolicyAuthorizer>,
    ) -> Self {
        self.remote_policy_authorizer = authorizer;
        self
    }

    fn into_parts(
        self,
    ) -> (
        ActorId,
        Option<Arc<dyn sync::CraqleGraphSync>>,
        Arc<dyn RemotePolicyAuthorizer>,
    ) {
        (self.actor, self.sync, self.remote_policy_authorizer)
    }
}

/// What one topic's reconcile pass applied, and the failure that stopped it.
/// Carried together so a stall cannot hide the prefix that landed before it.
#[derive(Default)]
struct TopicPass {
    applied: HashSet<GraphId>,
    stalled: Option<CraqleError>,
}

/// Reconcile passes an open makes before giving up.
const OPEN_RECONCILE_ATTEMPTS: usize = 3;
const OPEN_RECONCILE_BACKOFF: Duration = Duration::from_millis(50);

/// Catch a node up at open, retrying a stall a transient store or history
/// failure could clear rather than failing the open on it.
fn reconcile_at_open(node: &CraqleNode) -> Result<()> {
    for attempt in 1..OPEN_RECONCILE_ATTEMPTS {
        match node.reconcile_irokle() {
            Ok(_) => return Ok(()),
            Err(error) => {
                tracing::warn!(attempt, %error, "retrying a stalled reconcile at open");
                std::thread::sleep(OPEN_RECONCILE_BACKOFF);
            }
        }
    }
    node.reconcile_irokle().map(|_| ())
}

impl CraqleNode {
    /// Authoritative disk format validated when this node opened.
    pub fn disk_format_version(&self) -> DiskFormatVersion {
        DISK_FORMAT_VERSION
    }

    /// Open a node rooted at `path` with default options.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, CraqleOptions::default())
    }

    /// Open a node rooted at `path` with an explicit actor id.
    pub fn open_with_actor(path: impl AsRef<Path>, actor: ActorId) -> Result<Self> {
        Self::open_with_options(path, CraqleOptions::default().with_actor(actor))
    }

    /// Open a node rooted at `path` with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: CraqleOptions) -> Result<Self> {
        let root = path.as_ref();
        std::fs::create_dir_all(root)?;
        let search_storage = options.search_storage;
        let graph_store_persist_mode = options.graph_store_persist_mode;
        #[cfg(feature = "shacl-core")]
        let pending_replay_policy = options.pending_replay_policy;

        let store = Arc::new(GraphStore::open_with_persist_mode(
            root.join("store"),
            graph_store_persist_mode.into_store_mode(),
        )?);
        let search = Arc::new(match search_storage {
            SearchStorage::Disk => SearchIndex::open(root.join("search"))?,
            SearchStorage::Memory => SearchIndex::open_in_memory()?,
        });
        let search_needs_rebuild =
            search.needs_rebuild() || search_storage == SearchStorage::Memory;
        #[allow(unused_mut)]
        let mut node = Self::from_store_and_search(store, search.clone(), options);
        reconcile_at_open(&node)?;
        #[cfg(feature = "shacl-core")]
        {
            let startup_started = Instant::now();
            let mut outcome = PendingReplayOutcome::default();
            if node.store.pending_shacl_queue_repair_required()? {
                let repair = {
                    let _binding_guard = node.store.binding_guard();
                    node.store.repair_pending_shacl_queue()?
                };
                node.persist_fjall()?;
                outcome.statistics.binding_records_scanned = repair.binding_records_scanned;
                outcome.statistics.pending_queue_entries_scanned =
                    repair.pending_queue_entries_scanned;
            }
            let replay = match pending_replay_policy {
                PendingReplayPolicy::ReplayAllBeforeOpen => Some(
                    node.replication
                        .replay_pending_bindings_bounded(usize::MAX, None)?,
                ),
                PendingReplayPolicy::ReplayBounded {
                    max_graphs,
                    max_elapsed,
                } => Some(
                    node.replication
                        .replay_pending_bindings_bounded(max_graphs, Some(max_elapsed))?,
                ),
                PendingReplayPolicy::Defer => None,
            };
            if let Some(replay) = replay {
                outcome.statistics.pending_queue_entries_scanned +=
                    replay.statistics.pending_queue_entries_scanned;
                outcome.statistics.graphs_settled = replay.statistics.graphs_settled;
                outcome.statistics.reports_produced = replay.statistics.reports_produced;
                outcome.failures = replay.failures;
                outcome.budget_exhausted = replay.budget_exhausted;
            }
            outcome.statistics.elapsed = startup_started.elapsed();
            node.startup_pending_replay = outcome;
        }
        if search_needs_rebuild {
            node.schedule_full_search_reindex()?;
        }
        Ok(node)
    }

    pub fn from_store_and_search(
        store: Arc<GraphStore>,
        search: Arc<SearchIndex>,
        options: CraqleOptions,
    ) -> Self {
        let (actor, sync, remote_policy_authorizer) = options.into_parts();
        let search_worker = SearchUpdateWorker::start(store.clone(), search.clone());
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search.clone()));
        #[cfg(feature = "shacl-core")]
        let shacl = Arc::new(ShaclCompiler::new(store.clone()));
        #[cfg(feature = "shacl-core")]
        let local_replication = Arc::new(ReplicationEngine::new_with_shacl(
            store.clone(),
            sparql.clone(),
            actor,
            shacl.clone(),
        ));
        #[cfg(not(feature = "shacl-core"))]
        let local_replication =
            Arc::new(ReplicationEngine::new(store.clone(), sparql.clone(), actor));
        #[cfg(feature = "shacl-core")]
        let replication = Arc::new(if sync.is_some() {
            ReplicationEngine::new_sync_shacl(
                store.clone(),
                sparql.clone(),
                actor,
                sync.clone(),
                shacl.clone(),
            )
        } else {
            ReplicationEngine::new_with_shacl(store.clone(), sparql.clone(), actor, shacl.clone())
        });
        #[cfg(not(feature = "shacl-core"))]
        let replication = Arc::new(if sync.is_some() {
            ReplicationEngine::new_with_sync(store.clone(), sparql.clone(), actor, sync.clone())
        } else {
            ReplicationEngine::new(store.clone(), sparql.clone(), actor)
        });

        Self {
            actor,
            store,
            search,
            search_worker,
            sparql,
            #[cfg(feature = "shacl-core")]
            shacl,
            #[cfg(feature = "shacl-core")]
            startup_pending_replay: PendingReplayOutcome::default(),
            replication,
            local_replication,
            sync,
            remote_policy_authorizer,
            reconcile_guard: Mutex::new(()),
            replication_rejections: AtomicU64::new(0),
            #[cfg(test)]
            reindex_gate: std::sync::Mutex::new(None),
        }
    }

    /// Return the local actor id used for authored replication batches.
    pub fn actor(&self) -> ActorId {
        self.actor
    }

    #[cfg(feature = "shacl-core")]
    pub fn compile_shacl(
        &self,
        auth: &dyn Authorizer,
        shapes_graph: &GraphId,
        options: &ShaclCompileOptions,
    ) -> Result<CompiledShaclSchema> {
        self.authorize_shape_graphs(auth, shapes_graph)?;
        let schema = self.shacl.compile(shapes_graph, options)?;
        self.authorize_shape_versions(auth, schema.shape_versions())?;
        Ok(schema)
    }

    /// Compile a reusable native SHACL policy with immutable dependency fences.
    #[cfg(feature = "shacl-core")]
    pub fn compile_rocrate_policy(
        &self,
        auth: &dyn Authorizer,
        shapes_graph: &GraphId,
        options: &ShaclCompileOptions,
    ) -> Result<CompiledRoCratePolicy> {
        let shacl = self.compile_shacl(auth, shapes_graph, options)?;
        let root_shapes_version = shacl
            .shape_versions()
            .iter()
            .find_map(|(graph, version)| (graph == shapes_graph).then_some(*version))
            .ok_or_else(|| ShaclError::SchemaChangedDuringValidation {
                graph: shapes_graph.to_string(),
            })?;
        let imported_shapes = shacl
            .shape_versions()
            .iter()
            .filter(|(graph, _)| graph != shapes_graph)
            .cloned()
            .collect();
        Ok(CompiledRoCratePolicy {
            policy_id: rocrate_policy_id(shapes_graph, &shacl),
            shacl,
            compiler_model_version: SHACL_COMPILER_MODEL_VERSION,
            root_shapes_graph: shapes_graph.clone(),
            root_shapes_version,
            imported_shapes,
        })
    }

    /// Parse and encode one raw RO-Crate document without mutating source state.
    #[cfg(feature = "shacl-core")]
    pub fn prepare_rocrate_document(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        jsonld: &str,
        options: &PrepareRoCrateOptions,
    ) -> Result<PreparedRoCrateDocument> {
        self.ensure_policy_action(graph, &options.new_graph_policy, auth, Action::Write)?;
        Ok(self.manager().prepare_jsonld(graph, jsonld, options)?)
    }

    /// Evaluate one prepared candidate against structural RO-Crate rules and SHACL.
    #[cfg(feature = "shacl-core")]
    pub fn evaluate_rocrate_policy(
        &self,
        auth: &dyn Authorizer,
        document: &PreparedRoCrateDocument,
        policy: &CompiledRoCratePolicy,
        options: &RoCratePolicyOptions,
    ) -> Result<RoCratePolicyReport> {
        self.authorize_prepared_document(auth, document, Action::Read)?;
        self.authorize_shape_versions(auth, policy.shacl.shape_versions())?;
        if document.detected_version != policy.shacl.rocrate_version() {
            return Err(RoCrateError::VersionMismatch {
                first: document.detected_version,
                second: policy.shacl.rocrate_version(),
            }
            .into());
        }
        self.ensure_prepared_document_current(document)?;
        self.ensure_rocrate_policy_current(policy)?;

        let shacl = self.shacl.validate_delta(
            &document.graph,
            &policy.shacl,
            &document.encoded_changes,
            &options.validation,
        )?;

        self.ensure_prepared_document_current(document)?;
        self.ensure_rocrate_policy_current(policy)?;
        let conforms = document.structural_findings.is_empty() && shacl.conforms;
        let accepted_by_write_policy =
            document.structural_findings.is_empty() && shacl.accepted_by_write_policy;
        Ok(RoCratePolicyReport {
            conforms,
            accepted_by_write_policy,
            detected_version: document.detected_version,
            document_digest: document.document_digest,
            rocrate_violations: document.structural_findings.clone(),
            statistics: RoCratePolicyStatistics {
                parse_count: document.statistics.parse_count,
                parse_time: document.statistics.parse_time,
                encode_time: document.statistics.encode_time,
                structural_time: document.statistics.structural_time,
                diff_time: document.statistics.diff_time,
                target_time: shacl.statistics.target_time,
                constraint_time: shacl.statistics.constraint_time,
                report_time: shacl.statistics.report_time,
                encoded_triples: document.statistics.encoded_triples,
                encoded_changes: document.statistics.encoded_changes,
            },
            shacl,
        })
    }

    /// Commit a prepared candidate after rechecking its data and policy fences.
    #[cfg(feature = "shacl-core")]
    pub fn commit_prepared_rocrate_document(
        &self,
        auth: &dyn Authorizer,
        document: PreparedRoCrateDocument,
        policy: Option<&CompiledRoCratePolicy>,
        mode: PreparedCommitMode,
    ) -> Result<PreparedRoCrateCommitOutcome> {
        self.authorize_prepared_document(auth, &document, Action::Write)?;
        if !document.structural_findings.is_empty() {
            return Err(CraqleError::RoCrate(RoCrateError::Update(
                UpdateError::ValidationFailed(document.structural_findings.clone()),
            )));
        }
        let policy_report = match mode {
            PreparedCommitMode::Enforce | PreparedCommitMode::Advisory => {
                let policy = policy.ok_or(CraqleError::RoCratePolicyRequired { mode })?;
                let report = self.evaluate_rocrate_policy(
                    auth,
                    &document,
                    policy,
                    &RoCratePolicyOptions::default(),
                )?;
                if mode == PreparedCommitMode::Enforce && !report.accepted_by_write_policy {
                    return Err(CraqleError::RoCratePolicyRejected(Box::new(report)));
                }
                Some(report)
            }
            PreparedCommitMode::StructuralOnly => None,
        };

        let graph = document.graph.clone();
        let policy_to_persist = document.metadata.policy_to_persist.clone();
        let shape_versions = policy
            .map(|policy| policy.shacl.shape_versions())
            .unwrap_or_default();
        let batch = self.manager().commit_prepared(document, shape_versions)?;
        if let Some(policy) = policy_to_persist {
            self.persist_graph_policy(&graph, policy)?;
        }
        let batch = self.finish_batch(&graph, batch)?;
        Ok(PreparedRoCrateCommitOutcome {
            batch,
            policy_report,
        })
    }

    /// Prepare and evaluate a raw RO-Crate document with exactly one JSON parse.
    #[cfg(feature = "shacl-core")]
    pub fn validate_rocrate_document(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        jsonld: &str,
        policy: &CompiledRoCratePolicy,
        options: &RoCratePolicyOptions,
    ) -> Result<RoCratePolicyReport> {
        let document =
            self.prepare_rocrate_document(auth, graph, jsonld, &PrepareRoCrateOptions::default())?;
        self.evaluate_rocrate_policy(auth, &document, policy, options)
    }

    #[cfg(feature = "shacl-core")]
    pub fn validate_shacl(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
    ) -> Result<ShaclValidationReport> {
        self.shacl
            .validate_authorized(data_graph, schema, options, false, |view| {
                self.authorize_view_schema(view, auth, data_graph, schema)
            })
    }

    #[cfg(feature = "shacl-core")]
    pub fn validate_shacl_delta(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        changes: &[MaterializedQuadChange],
        options: &ShaclValidationOptions,
    ) -> Result<ShaclValidationReport> {
        self.shacl
            .validate_delta_authorized(data_graph, schema, changes, options, |view| {
                self.authorize_view_schema(view, auth, data_graph, schema)
            })
    }

    #[cfg(feature = "shacl-core")]
    pub fn bind_shacl(
        &self,
        auth: &dyn Authorizer,
        binding: &ShaclBinding,
    ) -> Result<ShaclBindingStatus> {
        let _commit_guard = self.store.graph_commit_guard(&binding.data_graph);
        self.ensure_graph_action(&binding.data_graph, auth, Action::Write)?;
        if binding.policy != ShaclWritePolicy::Disabled {
            self.ensure_graph_action(&binding.data_graph, auth, Action::Read)?;
        }
        self.authorize_shape_graphs(auth, &binding.shapes_graph)?;
        if !self.store.contains_graph(&binding.data_graph)? {
            return Err(store::StoreError::GraphNotFound(binding.data_graph.to_string()).into());
        }
        if !self.store.contains_graph(&binding.shapes_graph)? {
            return Err(store::StoreError::GraphNotFound(binding.shapes_graph.to_string()).into());
        }
        let mut completed = None;
        for _ in 0..3 {
            let data_version = self.store.graph_version_digest(&binding.data_graph)?;
            let shapes_version = self.store.graph_version_digest(&binding.shapes_graph)?;
            let status = if binding.policy == ShaclWritePolicy::Disabled {
                ShaclBindingStatus {
                    binding: binding.clone(),
                    state: ShaclValidationState::Pending,
                    report: None,
                    error: None,
                    data_version,
                    shapes_version,
                    schema_fingerprint: [0; 32],
                    compiler_model_version: SHACL_COMPILER_MODEL_VERSION,
                    shape_versions: vec![(binding.shapes_graph.clone(), shapes_version)],
                }
            } else {
                let schema = self.shacl.compile(
                    &binding.shapes_graph,
                    &binding.validation_options.compile_options(),
                )?;
                self.authorize_shape_versions(auth, schema.shape_versions())?;
                let report = self.shacl.validate(
                    &binding.data_graph,
                    &schema,
                    &binding.validation_options.validation_options(),
                    false,
                )?;
                ShaclBindingStatus {
                    binding: binding.clone(),
                    state: if report.conforms {
                        ShaclValidationState::Valid
                    } else {
                        ShaclValidationState::Invalid
                    },
                    report: Some(report),
                    error: None,
                    data_version,
                    shapes_version,
                    schema_fingerprint: schema.plan_fingerprint(),
                    compiler_model_version: SHACL_COMPILER_MODEL_VERSION,
                    shape_versions: schema.shape_versions().to_vec(),
                }
            };
            let binding_guard = self.store.binding_guard();
            if self.store.graph_version_digest(&binding.data_graph)? == data_version
                && self.store.graph_version_digest(&binding.shapes_graph)? == shapes_version
                && self.shacl.versions_are_current(&status.shape_versions)?
            {
                completed = Some((binding_guard, status));
                break;
            }
        }
        let (_binding_guard, status) =
            completed.ok_or_else(|| ShaclError::SchemaChangedDuringValidation {
                graph: binding.shapes_graph.to_string(),
            })?;
        let mut batch = self.store.new_batch();
        self.store.stage_binding_status(&mut batch, &status)?;
        self.store.commit(batch)?;
        self.persist_fjall()?;
        Ok(status)
    }

    #[cfg(feature = "shacl-core")]
    pub fn unbind_shacl(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
        shapes_graph: &GraphId,
    ) -> Result<()> {
        let _commit_guard = self.store.graph_commit_guard(data_graph);
        self.ensure_graph_action(data_graph, auth, Action::Write)?;
        let _binding_guard = self.store.binding_guard();
        let mut batch = self.store.new_batch();
        self.store
            .stage_binding_remove(&mut batch, data_graph, shapes_graph)?;
        self.store.commit(batch)?;
        self.persist_fjall()?;
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub fn shacl_bindings(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
    ) -> Result<Vec<ShaclBinding>> {
        Ok(self
            .shacl_binding_statuses(auth, data_graph)?
            .into_iter()
            .map(|status| status.binding)
            .collect())
    }

    #[cfg(feature = "shacl-core")]
    pub fn shacl_binding_statuses(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
    ) -> Result<Vec<ShaclBindingStatus>> {
        self.ensure_graph_action(data_graph, auth, Action::Read)?;
        let mut statuses = {
            let _binding_guard = self.store.binding_guard();
            self.store.shacl_binding_statuses(data_graph)?
        };
        let mut version_checks = 0u64;
        for status in &mut statuses {
            self.ensure_graph_action(&status.binding.shapes_graph, auth, Action::Read)?;
            for (graph, _) in &status.shape_versions {
                if graph != &status.binding.shapes_graph {
                    self.ensure_graph_action(graph, auth, Action::Read)?;
                }
            }
            if status.binding.policy == ShaclWritePolicy::Disabled {
                if matches!(
                    status.state,
                    ShaclValidationState::Valid | ShaclValidationState::Invalid
                ) {
                    status.state = ShaclValidationState::Pending;
                    status.report = None;
                    status.error = None;
                }
                continue;
            }
            version_checks += 1 + status.shape_versions.len() as u64;
            let root_recorded = status.shape_versions.iter().any(|(graph, version)| {
                graph == &status.binding.shapes_graph && *version == status.shapes_version
            });
            let current = self.store.contains_graph(&status.binding.data_graph)?
                && status.data_version
                    == self
                        .store
                        .graph_version_digest(&status.binding.data_graph)?
                && root_recorded
                && status.compiler_model_version == SHACL_COMPILER_MODEL_VERSION
                && self.shacl.versions_are_current(&status.shape_versions)?;
            if !current {
                status.state = ShaclValidationState::Pending;
                status.report = None;
                status.error = None;
            }
        }
        self.store
            .record_status_read(statuses.len() as u64, version_checks);
        Ok(statuses)
    }

    #[cfg(feature = "shacl-core")]
    pub fn startup_pending_replay(&self) -> &PendingReplayOutcome {
        &self.startup_pending_replay
    }

    #[cfg(feature = "shacl-core")]
    pub fn pending_shacl_queue(&self) -> Result<Vec<GraphId>> {
        Ok(self.store.pending_shacl_queue()?)
    }

    #[cfg(feature = "shacl-core")]
    pub fn pending_shacl_queue_status(&self) -> Result<PendingShaclQueueStatus> {
        let runtime = self.store.shacl_runtime_statistics();
        Ok(PendingShaclQueueStatus {
            pending_count: self.store.pending_shacl_count()?,
            settlement_failures: runtime.settlement_failures,
        })
    }

    #[cfg(feature = "shacl-core")]
    pub fn replay_pending_shacl(
        &self,
        max_graphs: usize,
        max_elapsed: Duration,
    ) -> Result<PendingReplayOutcome> {
        Ok(self
            .replication
            .replay_pending_bindings_bounded(max_graphs, Some(max_elapsed))?)
    }

    #[cfg(feature = "shacl-core")]
    pub fn repair_pending_shacl_queue(&self) -> Result<PendingReplayStatistics> {
        let started = Instant::now();
        let repair = {
            let _binding_guard = self.store.binding_guard();
            self.store.repair_pending_shacl_queue()?
        };
        self.persist_fjall()?;
        Ok(PendingReplayStatistics {
            binding_records_scanned: repair.binding_records_scanned,
            pending_queue_entries_scanned: repair.pending_queue_entries_scanned,
            elapsed: started.elapsed(),
            ..PendingReplayStatistics::default()
        })
    }

    #[cfg(feature = "shacl-core")]
    pub fn shacl_runtime_statistics(&self) -> ShaclRuntimeStatistics {
        self.store.shacl_runtime_statistics()
    }

    #[cfg(feature = "shacl-core")]
    pub fn conforms_shacl(
        &self,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
    ) -> Result<bool> {
        Ok(self
            .shacl
            .validate_authorized(data_graph, schema, options, true, |view| {
                self.authorize_view_schema(view, auth, data_graph, schema)
            })?
            .conforms)
    }

    #[cfg(feature = "shacl-core")]
    fn authorize_view_schema(
        &self,
        view: &StoreReadView<'_>,
        auth: &dyn Authorizer,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
    ) -> Result<()> {
        self.authorize_view_action(view, auth, data_graph, Action::Read)?;
        for (graph, _) in schema.shape_versions() {
            self.authorize_view_action(view, auth, graph, Action::Read)?;
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn authorize_view_action(
        &self,
        view: &StoreReadView<'_>,
        auth: &dyn Authorizer,
        graph: &GraphId,
        action: Action,
    ) -> Result<()> {
        let policy = view
            .snapshot()
            .graph_policy(view.store(), graph)?
            .unwrap_or_default();
        auth.authorize(graph, &policy, action)?;
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn authorize_shape_versions(
        &self,
        auth: &dyn Authorizer,
        shape_versions: &[(GraphId, [u8; 32])],
    ) -> Result<()> {
        for (graph, _) in shape_versions {
            self.ensure_graph_action(graph, auth, Action::Read)?;
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn authorize_prepared_document(
        &self,
        auth: &dyn Authorizer,
        document: &PreparedRoCrateDocument,
        action: Action,
    ) -> Result<()> {
        let policy = match &document.base {
            PreparedGraphBase::New => document
                .metadata
                .policy_to_persist
                .clone()
                .unwrap_or_default(),
            PreparedGraphBase::Existing { .. } => self.store.graph_policy(&document.graph)?,
        };
        auth.authorize(&document.graph, &policy, action)?;
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn ensure_prepared_document_current(&self, document: &PreparedRoCrateDocument) -> Result<()> {
        let current = match &document.base {
            PreparedGraphBase::New => !self.store.contains_graph(&document.graph)?,
            PreparedGraphBase::Existing { data_version } => {
                self.store.contains_graph(&document.graph)?
                    && self.store.graph_version_digest(&document.graph)? == *data_version
            }
        };
        if current {
            Ok(())
        } else {
            Err(RoCrateError::StalePreparedState {
                fence: "data graph version".to_owned(),
            }
            .into())
        }
    }

    #[cfg(feature = "shacl-core")]
    fn ensure_rocrate_policy_current(&self, policy: &CompiledRoCratePolicy) -> Result<()> {
        let shape_versions = policy.shacl.shape_versions();
        if policy.compiler_model_version != SHACL_COMPILER_MODEL_VERSION
            || policy.shacl.model_version() != SHACL_COMPILER_MODEL_VERSION
        {
            return Err(RoCrateError::StalePreparedState {
                fence: "compiler model version".to_owned(),
            }
            .into());
        }
        if policy.policy_id != rocrate_policy_id(&policy.root_shapes_graph, &policy.shacl) {
            return Err(RoCrateError::StalePreparedState {
                fence: "compiled policy identity".to_owned(),
            }
            .into());
        }
        let root_matches = shape_versions.iter().any(|(graph, version)| {
            graph == &policy.root_shapes_graph && version == &policy.root_shapes_version
        });
        let imported_shapes = shape_versions
            .iter()
            .filter(|(graph, _)| graph != &policy.root_shapes_graph)
            .cloned()
            .collect::<Vec<_>>();
        if !root_matches || imported_shapes != policy.imported_shapes {
            return Err(RoCrateError::StalePreparedState {
                fence: "compiled policy shape-version set".to_owned(),
            }
            .into());
        }
        for (graph, expected) in shape_versions {
            if !self.store.contains_graph(graph)?
                || self.store.graph_version_digest(graph)? != *expected
            {
                return Err(RoCrateError::StalePreparedState {
                    fence: format!("shapes graph `{}` version", graph.as_str()),
                }
                .into());
            }
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn authorize_shape_graphs(&self, auth: &dyn Authorizer, shapes_graph: &GraphId) -> Result<()> {
        const OWL_IMPORTS: &str = "<http://www.w3.org/2002/07/owl#imports>";

        let view = StoreReadView::new(&self.store);
        let context = ReadContext::for_validation(QueryCancellation::new(), shapes_graph);
        let imports_id = hash_term(&EncodedTerm(OWL_IMPORTS.to_owned()));
        let mut pending = vec![shapes_graph.clone()];
        let mut visited = HashSet::new();
        while let Some(graph) = pending.pop() {
            if !visited.insert(graph.to_string()) {
                continue;
            }
            self.ensure_graph_action(&graph, auth, Action::Read)?;
            if !view.contains_graph(&graph)? {
                continue;
            }
            let graph_id = hash_term(&EncodedTerm::from_named_node(&graph.0));
            for quad in view.scan(
                &context,
                GraphSelector::Named(graph_id),
                QuadPattern {
                    predicate: Some(imports_id),
                    ..QuadPattern::default()
                },
            )? {
                let object = view.decode_term(&context, quad?.object)?;
                let Some(import) = object.to_named_node() else {
                    continue;
                };
                let imported = GraphId::new(import.as_str());
                if view.contains_graph(&imported)? {
                    pending.push(imported);
                }
            }
        }
        Ok(())
    }

    /// Return the Fjall persistence mode used for explicit graph-store persists.
    pub fn graph_store_persist_mode(&self) -> CraqleFjallPersistMode {
        CraqleFjallPersistMode::from_store_mode(self.store.persist_mode())
    }

    pub fn irokle_topic_id(&self, graph: &GraphId) -> Result<Option<irokle::TopicId>> {
        let Some(sync) = &self.sync else {
            return Ok(None);
        };
        Ok(sync.graph_topic_id(&self.store, graph)?)
    }

    pub fn ensure_irokle_topic(&self, graph: &GraphId) -> Result<irokle::TopicId> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic_id = sync.ensure_graph_topic(&self.store, graph)?;
        self.persist_fjall()?;
        Ok(topic_id)
    }

    /// Deterministic graph topic id, binding it locally only if its genesis is
    /// already present. Never mints, so concurrent callers on different nodes
    /// cannot fork rival geneses for the same graph.
    pub fn bind_or_derive_irokle_topic(&self, graph: &GraphId) -> Result<irokle::TopicId> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        if let Some(topic_id) = sync.bind_graph_topic_if_present(&self.store, graph)? {
            self.persist_fjall()?;
            return Ok(topic_id);
        }
        Ok(crate::sync::graph_topic_id(graph))
    }

    /// Binds the graph's topic id if its genesis is present locally, else `None`.
    pub fn bind_irokle_topic(&self, graph: &GraphId) -> Result<Option<irokle::TopicId>> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let bound = sync.bind_graph_topic_if_present(&self.store, graph)?;
        if bound.is_some() {
            self.persist_fjall()?;
        }
        Ok(bound)
    }

    /// Mints the graph's topic genesis with an explicit member set (or binds an
    /// existing one). The only path that creates a graph genesis; callers own
    /// the single-minter discipline.
    pub fn mint_irokle_topic(
        &self,
        graph: &GraphId,
        initial_peers: std::collections::BTreeSet<irokle::PeerId>,
    ) -> Result<irokle::TopicId> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic_id = sync.mint_graph_topic(&self.store, graph, initial_peers)?;
        self.persist_fjall()?;
        Ok(topic_id)
    }

    pub fn add_irokle_peer(&self, graph: &GraphId, peer: irokle::PeerId) -> Result<()> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        sync.add_peer(&self.store, graph, peer)?;
        self.persist_fjall()
    }

    pub fn remove_irokle_peer(&self, graph: &GraphId, peer: irokle::PeerId) -> Result<()> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        sync.remove_peer(&self.store, graph, peer)?;
        self.persist_fjall()
    }

    pub fn irokle_sync_status(&self, graph: &GraphId) -> Result<Vec<irokle::SyncPeerStatus>> {
        let Some(sync) = &self.sync else {
            return Ok(Vec::new());
        };
        Ok(sync.sync_status(&self.store, graph)?)
    }

    /// Apply every craqle topic's outstanding records, returning the graphs
    /// whose content changed. Callers that only want a count read `.len()`.
    pub fn reconcile_irokle(&self) -> Result<HashSet<GraphId>> {
        let Some(sync) = &self.sync else {
            return Ok(HashSet::new());
        };
        let _reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut applied = HashSet::new();
        let mut stalled = None;
        // Topics carry independent cursors, so one stall holds back only its
        // own topic; the first failure is reported once the rest have run.
        for topic_id in sync.craqle_topic_ids()? {
            match self.reconcile_irokle_topic(sync, topic_id) {
                Ok(pass) => {
                    applied.extend(pass.applied);
                    stalled = stalled.or(pass.stalled);
                }
                Err(error) => stalled = stalled.or(Some(error)),
            }
        }
        // Before the stall reaches the caller: a pass that applied a prefix
        // owes that prefix and its cursor the configured durability.
        if !applied.is_empty() {
            self.persist_fjall()?;
        }
        match stalled {
            Some(error) => Err(error),
            None => Ok(applied),
        }
    }

    /// Apply a topic's outstanding records in order, stopping at the first
    /// failure a retry could clear rather than losing that record for good.
    fn reconcile_irokle_topic(
        &self,
        sync: &Arc<dyn sync::CraqleGraphSync>,
        topic_id: irokle::TopicId,
    ) -> Result<TopicPass> {
        let stored_cursor = self.store.applied_topic_clock(topic_id.as_bytes())?;
        // A history read that fails is retryable, so it stalls its topic. A
        // silent skip would leave the topic unread for the rest of the process.
        let catchup = sync.topic_records_since(topic_id, stored_cursor.as_deref())?;

        let sync::TopicCatchup {
            records,
            mut cursor,
        } = catchup;

        let mut applied = HashSet::new();
        let mut stalled = None;
        for topic_record in &records {
            if let sync::TopicRecord::Rejected(record) = topic_record {
                cursor.consume(topic_record);
                let cursor_bytes = cursor
                    .encode()?
                    .expect("a consumed replication record has a cursor");
                let graph = self
                    .store
                    .topic_graph_binding(topic_id.as_bytes())?
                    .map(|graph| GraphId::new(&graph));
                let rejection = RejectedReplicationRecord {
                    topic: topic_id,
                    record_id: record.meta.op_id,
                    actor: record.meta.actor_id,
                    sequence: record.meta.actor_seq,
                    graph,
                    payload_digest: record.payload_digest,
                    error_kind: record.error_kind,
                    reason: record.reason.clone(),
                    seen_count: 0,
                    acknowledged: false,
                };
                let rejection = self
                    .store
                    .record_replication_rejection(rejection, Some(&cursor_bytes))?;
                self.store.persist()?;
                self.replication_rejections.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    topic = %topic_id,
                    record = %rejection.record_id,
                    error_kind = ?rejection.error_kind,
                    seen_count = rejection.seen_count,
                    "persisted rejected craqle replication payload before cursor advance",
                );
                continue;
            }
            let sync::TopicRecord::Event(record) = topic_record else {
                unreachable!()
            };
            match self.apply_reconciled_record(sync, topic_id, record) {
                Ok(Some(graph)) => {
                    applied.insert(graph);
                    cursor.consume(topic_record);
                }
                Ok(None) => cursor.consume(topic_record),
                Err(error) if error.rejects_record() => {
                    cursor.consume(topic_record);
                    let cursor_bytes = cursor
                        .encode()?
                        .expect("a consumed replication record has a cursor");
                    let rejection = self.rejected_replication_record(topic_id, record, &error)?;
                    let rejection = self
                        .store
                        .record_replication_rejection(rejection, Some(&cursor_bytes))?;
                    self.store.persist()?;
                    self.replication_rejections.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        topic = %topic_id,
                        record = %rejection.record_id,
                        error_kind = ?rejection.error_kind,
                        seen_count = rejection.seen_count,
                        "persisted rejected craqle replication record before cursor advance",
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        topic = %topic_id,
                        %error,
                        "stalled craqle reconcile at a retryable failure",
                    );
                    stalled = Some(error);
                    break;
                }
            }
        }

        // Persisted even when the pass stalled: it covers exactly the prefix
        // that was consumed, so the retry resumes at the failed record.
        if let Some(cursor) = cursor.encode()? {
            self.store
                .set_applied_topic_clock(topic_id.as_bytes(), &cursor)?;
        }
        Ok(TopicPass { applied, stalled })
    }

    /// Applies one record, naming the graph it changed when it changed one.
    fn apply_reconciled_record(
        &self,
        sync: &Arc<dyn sync::CraqleGraphSync>,
        topic_id: irokle::TopicId,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<Option<GraphId>> {
        let graph = record.event.graph();
        match self.store.topic_graph_binding(topic_id.as_bytes())? {
            Some(bound) if bound != graph.as_str() => {
                tracing::warn!(
                    topic = %topic_id,
                    bound = %bound,
                    claimed = %graph.as_str(),
                    "rejected craqle record targeting a graph outside its topic binding",
                );
                return Err(CraqleError::ReplicationRejected {
                    error_kind: CraqleErrorKind::CorruptAuthoritativeData,
                    reason: "record targets a graph outside its topic binding".to_owned(),
                });
            }
            Some(_) => {}
            None => sync.bind_graph_topic(&self.store, graph, topic_id)?,
        }
        let local_record = sync.is_local_record(topic_id, record);
        let _write_guard = replication::graph_write_guard(graph);
        Ok(self
            .apply_irokle_record_locked(record, local_record)?
            .then(|| graph.clone()))
    }

    fn rejected_replication_record(
        &self,
        topic: irokle::TopicId,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
        error: &CraqleError,
    ) -> Result<RejectedReplicationRecord> {
        let payload = postcard::to_allocvec(&record.event)
            .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))?;
        let reason = match error {
            CraqleError::ReplicationRejected { reason, .. } => reason.clone(),
            CraqleError::Merge(MergeError::InputRejected(_))
            | CraqleError::SyncInputRejected(_) => "malformed graph event".to_owned(),
            CraqleError::Merge(MergeError::Store(_)) | CraqleError::Store(_) => {
                "record rejected for authoritative corruption".to_owned()
            }
            CraqleError::Sync(_) => "unsupported or malformed graph-event record".to_owned(),
            _ => "replication record rejected".to_owned(),
        };
        Ok(RejectedReplicationRecord {
            topic,
            record_id: record.meta.op_id,
            actor: record.meta.actor_id,
            sequence: record.meta.actor_seq,
            graph: Some(record.event.graph().clone()),
            payload_digest: *blake3::hash(&payload).as_bytes(),
            error_kind: error.kind(),
            reason,
            seen_count: 0,
            acknowledged: false,
        })
    }

    pub fn replication_rejection_count(&self) -> u64 {
        self.replication_rejections.load(Ordering::Relaxed)
    }

    pub fn list_rejected_replication_records(
        &self,
        auth: &dyn Authorizer,
    ) -> Result<Vec<RejectedReplicationRecord>> {
        let records = self.store.replication_rejections()?;
        for record in &records {
            self.authorize_rejection_record(auth, record, Action::Read)?;
        }
        Ok(records)
    }

    pub fn inspect_rejected_replication_record(
        &self,
        auth: &dyn Authorizer,
        topic: irokle::TopicId,
        record_id: irokle::OpId,
    ) -> Result<Option<RejectedReplicationRecord>> {
        let Some(record) = self.store.replication_rejection(&topic, &record_id)? else {
            return Ok(None);
        };
        self.authorize_rejection_record(auth, &record, Action::Read)?;
        Ok(Some(record))
    }

    pub fn retry_rejected_replication_record(
        &self,
        auth: &dyn Authorizer,
        topic: irokle::TopicId,
        record_id: irokle::OpId,
    ) -> Result<bool> {
        let _reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(rejected) = self.store.replication_rejection(&topic, &record_id)? else {
            return Ok(false);
        };
        self.authorize_rejection_record(auth, &rejected, Action::Write)?;
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic_record = sync
            .topic_records_since(topic, None)?
            .records
            .into_iter()
            .find(|record| record.meta().op_id == record_id)
            .ok_or_else(|| {
                CraqleError::SyncInputRejected(format!(
                    "rejected record {record_id} is no longer available in topic {topic}"
                ))
            })?;
        let record = match topic_record {
            sync::TopicRecord::Event(record) => record,
            sync::TopicRecord::Rejected(record) => {
                let rejection = RejectedReplicationRecord {
                    topic,
                    record_id: record.meta.op_id,
                    actor: record.meta.actor_id,
                    sequence: record.meta.actor_seq,
                    graph: rejected.graph,
                    payload_digest: record.payload_digest,
                    error_kind: record.error_kind,
                    reason: record.reason.clone(),
                    seen_count: 0,
                    acknowledged: false,
                };
                self.store.record_replication_rejection(rejection, None)?;
                self.persist_fjall()?;
                self.replication_rejections.fetch_add(1, Ordering::Relaxed);
                return Err(CraqleError::ReplicationRejected {
                    error_kind: record.error_kind,
                    reason: record.reason,
                });
            }
        };
        match self.apply_reconciled_record(sync, topic, &record) {
            Ok(_) => {
                self.store
                    .delete_replication_rejection(&topic, &record_id)?;
                self.persist_fjall()?;
                Ok(true)
            }
            Err(error) if error.rejects_record() => {
                let rejection = self.rejected_replication_record(topic, &record, &error)?;
                self.store.record_replication_rejection(rejection, None)?;
                self.persist_fjall()?;
                self.replication_rejections.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    pub fn acknowledge_rejected_replication_record(
        &self,
        auth: &dyn Authorizer,
        topic: irokle::TopicId,
        record_id: irokle::OpId,
    ) -> Result<bool> {
        let _reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = self.store.replication_rejection(&topic, &record_id)? else {
            return Ok(false);
        };
        self.authorize_rejection_record(auth, &record, Action::Write)?;
        let changed = self
            .store
            .acknowledge_replication_rejection(&topic, &record_id)?;
        if changed {
            self.persist_fjall()?;
        }
        Ok(changed)
    }

    pub fn delete_rejected_replication_record(
        &self,
        auth: &dyn Authorizer,
        topic: irokle::TopicId,
        record_id: irokle::OpId,
    ) -> Result<bool> {
        let _reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = self.store.replication_rejection(&topic, &record_id)? else {
            return Ok(false);
        };
        self.authorize_rejection_record(auth, &record, Action::Write)?;
        let changed = self
            .store
            .delete_replication_rejection(&topic, &record_id)?;
        if changed {
            self.persist_fjall()?;
        }
        Ok(changed)
    }

    pub fn repair_irokle_topic_cursor(
        &self,
        auth: &dyn Authorizer,
        topic: irokle::TopicId,
        expected_old_cursor_digest: [u8; 32],
        replacement_position: irokle::ActorClock,
    ) -> Result<TopicCursorRepairAudit> {
        let _reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let graph = self
            .store
            .topic_graph_binding(topic.as_bytes())?
            .map(|graph| GraphId::new(&graph))
            .ok_or_else(|| {
                CraqleError::SyncInputRejected(format!(
                    "irokle topic {topic} is not bound to a graph"
                ))
            })?;
        self.ensure_graph_action(&graph, auth, Action::Write)?;
        let replacement = sync::encode_topic_cursor(topic, &replacement_position)?;
        let audit = self.store.repair_topic_cursor(
            topic,
            expected_old_cursor_digest,
            &replacement,
            Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        )?;
        self.persist_fjall()?;
        tracing::warn!(
            topic = %topic,
            old_cursor_digest = ?audit.old_cursor_digest,
            replacement_cursor_digest = ?audit.replacement_cursor_digest,
            "repaired authoritative replication cursor",
        );
        Ok(audit)
    }

    fn authorize_rejection_record(
        &self,
        auth: &dyn Authorizer,
        record: &RejectedReplicationRecord,
        action: Action,
    ) -> Result<()> {
        let graph = record.graph.as_ref().ok_or_else(|| {
            CraqleError::SyncInputRejected(
                "unbound replication rejection requires graph context".to_owned(),
            )
        })?;
        self.ensure_graph_action(graph, auth, action)
    }

    pub fn graph_policy(&self, graph: &GraphId) -> Result<GraphPolicy> {
        Ok(self.store.graph_policy(graph)?)
    }

    pub fn graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        Ok(self.store.graph_diagnostics(graph)?)
    }

    pub fn graph_violations(&self, graph: &GraphId) -> Result<Vec<CrateViolation>> {
        Ok(crate::rules::post_merge_violations_from_store(
            &self.store,
            graph,
        )?)
    }

    /// Return the RO-Crate version from its live marker or retained context evidence.
    pub fn crate_version(&self, graph: &GraphId) -> Result<RoCrateVersion> {
        Ok(self.manager().crate_version(graph)?)
    }

    /// Create a new RO-Crate graph.
    pub fn create_crate(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
    ) -> Result<Batch> {
        self.create_crate_with_durability(auth, request, CraqleRequestDurability::Durable)
    }

    /// Create a new RO-Crate graph with an explicit schema version and optional
    /// license override.
    pub fn create_crate_with_options(
        &self,
        auth: &dyn Authorizer,
        mut request: CreateCrateRequest,
        options: CreateCrateOptions,
    ) -> Result<Batch> {
        if let Some(license) = options.license {
            request.license = Some(license);
        }
        self.create_crate_with_durability_as_version(
            auth,
            request,
            CraqleRequestDurability::Durable,
            None,
            options.version,
        )
    }

    /// Create a new RO-Crate graph with an explicit request durability policy.
    pub fn create_crate_with_durability(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
        durability: CraqleRequestDurability,
    ) -> Result<Batch> {
        self.create_crate_with_durability_as(auth, request, durability, None)
    }

    /// Like [`CraqleNode::create_crate_with_durability`], but non-publishing
    /// writes are authored under `actor`, so replicas materializing the same
    /// logical event emit identical CRDT ops.
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %request.graph.as_str()))]
    pub fn create_crate_with_durability_as(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        self.create_crate_with_durability_as_version(
            auth,
            request,
            durability,
            actor,
            RoCrateVersion::default(),
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %request.graph.as_str()))]
    fn create_crate_with_durability_as_version(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
        version: RoCrateVersion,
    ) -> Result<Batch> {
        let CreateCrateRequest {
            graph,
            name,
            description,
            date_published,
            license,
            policy,
        } = request;
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let manager = self.manager_with(durability, actor);
        let batch = if version == RoCrateVersion::default() {
            manager.create_crate(
                graph.clone(),
                &name,
                &description,
                &date_published,
                license.as_deref(),
            )?
        } else {
            manager.create_crate_with_version(
                graph.clone(),
                &name,
                &description,
                &date_published,
                license.as_deref(),
                version,
            )?
        };
        self.persist_graph_policy_with_durability(&graph, policy, durability)?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Create a crate from a scaffold request that was already validated at
    /// its origin, skipping post-state rule re-validation.
    ///
    /// The request fields must be identical to ones that passed
    /// `validate_create_crate` (or the checked create) at the origin. Use the
    /// checked variant for any untrusted input.
    pub fn create_crate_prevalidated_with_durability_as(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        let CreateCrateRequest {
            graph,
            name,
            description,
            date_published,
            license,
            policy,
        } = request;
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager_with(durability, actor)
            .create_crate_prevalidated(
                graph.clone(),
                &name,
                &description,
                &date_published,
                license.as_deref(),
            )?;
        self.persist_graph_policy_with_durability(&graph, policy, durability)?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Validate and materialize a create-crate request without applying it.
    ///
    /// Returns the changes that would be applied, but does not mutate the graph
    /// store, persist policy, enqueue search, or publish Irokle records.
    pub fn validate_create_crate(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
    ) -> Result<Vec<CoreMaterializedQuadChange>> {
        let CreateCrateRequest {
            graph,
            name,
            description,
            date_published,
            license,
            policy,
        } = request;
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        Ok(self.manager().validate_create_crate(
            &graph,
            &name,
            &description,
            &date_published,
            license.as_deref(),
        )?)
    }

    /// Create or replace a root-linked data entity using a typed request.
    pub fn add_data_entity_with(
        &self,
        auth: &dyn Authorizer,
        request: CreateEntityRequest,
    ) -> Result<Batch> {
        self.add_data_entity_with_triples(
            auth,
            &request.graph,
            &request.entity_id,
            &request.entity_type,
            &request.name,
            request.additional_triples,
        )
    }

    /// Patch one root-linked data entity with explicit durability and actor.
    pub fn patch_data_with(
        &self,
        auth: &dyn Authorizer,
        request: PatchEntityRequest,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        let PatchEntityRequest {
            entity,
            replaced_predicates,
        } = request;
        let graph = entity.graph;
        self.ensure_graph_action(&graph, auth, Action::Write)?;
        let batch = self.manager_with(durability, actor).patch_data_entity(
            &graph,
            &entity.entity_id,
            &entity.entity_type,
            &entity.name,
            entity.additional_triples,
            &replaced_predicates,
        )?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Create or replace a root-linked data entity.
    pub fn add_data_entity(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
    ) -> Result<Batch> {
        self.add_data_entity_with_triples(auth, graph, entity_id, entity_type, name, Vec::new())
    }

    /// Create or replace a root-linked data entity with extra RDF properties.
    pub fn add_data_entity_with_triples(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> Result<Batch> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let batch = self.manager().add_data_entity(
            graph,
            entity_id,
            entity_type,
            name,
            additional_triples,
        )?;
        self.finish_batch(graph, batch)
    }

    /// Append many new root-linked data entities in one committed batch.
    pub fn append_new_root_data_entities(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let report = self
            .manager()
            .append_new_root_data_entities(graph, entities)?;
        self.finish_report(graph, report)
    }

    /// Append many new child data entities under an existing parent entity.
    pub fn append_new_data_entities_under(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        parent_id: &str,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let report = self
            .manager()
            .append_new_data_entities_under(graph, parent_id, entities)?;
        self.finish_report(graph, report)
    }

    /// Create or replace a contextual entity using a typed request.
    pub fn add_contextual_entity_with(
        &self,
        auth: &dyn Authorizer,
        request: CreateEntityRequest,
    ) -> Result<Batch> {
        self.add_contextual_entity_with_triples(
            auth,
            &request.graph,
            &request.entity_id,
            &request.entity_type,
            &request.name,
            request.additional_triples,
        )
    }

    /// Patch one contextual entity with explicit durability and actor.
    pub fn patch_contextual_with(
        &self,
        auth: &dyn Authorizer,
        request: PatchEntityRequest,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        let PatchEntityRequest {
            entity,
            replaced_predicates,
        } = request;
        let graph = entity.graph;
        self.ensure_graph_action(&graph, auth, Action::Write)?;
        let batch = self
            .manager_with(durability, actor)
            .patch_contextual_entity(
                &graph,
                &entity.entity_id,
                &entity.entity_type,
                &entity.name,
                entity.additional_triples,
                &replaced_predicates,
            )?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Create or replace a contextual entity.
    pub fn add_contextual_entity(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
    ) -> Result<Batch> {
        self.add_contextual_entity_with_triples(
            auth,
            graph,
            entity_id,
            entity_type,
            name,
            Vec::new(),
        )
    }

    /// Create or replace a contextual entity with extra RDF properties.
    pub fn add_contextual_entity_with_triples(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> Result<Batch> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let batch = self.manager().add_contextual_entity(
            graph,
            entity_id,
            entity_type,
            name,
            additional_triples,
        )?;
        self.finish_batch(graph, batch)
    }

    /// Set the hidden access policy for a graph.
    pub fn set_graph_policy(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        policy: GraphPolicy,
    ) -> Result<()> {
        let policy = policy.normalized();
        self.ensure_policy_action(graph, &policy, auth, Action::Write)?;
        self.persist_graph_policy(graph, policy)?;
        self.persist_fjall()
    }

    /// Export the full visible RO-Crate as JSON-LD.
    pub fn export_rocrate(&self, auth: &dyn Authorizer, graph: &GraphId) -> Result<String> {
        self.ensure_graph_action(graph, auth, Action::Read)?;
        Ok(self.manager().export_jsonld(graph)?)
    }

    /// Export a summary JSON-LD view without paged data entities.
    pub fn export_rocrate_summary(&self, auth: &dyn Authorizer, graph: &GraphId) -> Result<String> {
        self.ensure_graph_action(graph, auth, Action::Read)?;
        Ok(self.manager().export_jsonld_summary(graph)?)
    }

    /// Export a paged JSON-LD view using an offset cursor.
    pub fn export_rocrate_page(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        offset: usize,
        limit: usize,
    ) -> Result<RoCratePage> {
        self.ensure_graph_action(graph, auth, Action::Read)?;
        Ok(self.manager().export_jsonld_page(graph, offset, limit)?)
    }

    /// Export a paged JSON-LD view using a versioned opaque cursor returned by
    /// the preceding page.
    pub fn export_rocrate_page_after(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RoCratePage> {
        self.ensure_graph_action(graph, auth, Action::Read)?;
        Ok(self
            .manager()
            .export_jsonld_page_after(graph, cursor, limit)?)
    }

    /// Replace the current visible RO-Crate state from a JSON-LD document.
    pub fn apply_rocrate_document(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
    ) -> Result<Batch> {
        self.ensure_graph_action(&graph, auth, Action::Write)?;
        let batch = self.manager().import_jsonld(graph.clone(), jsonld)?;
        self.finish_batch(&graph, batch)
    }

    /// Create or replace a visible RO-Crate state from a JSON-LD document and
    /// persist graph policy when bootstrapping a new graph.
    ///
    /// New or empty graphs automatically take the trusted bootstrap fast path.
    pub fn apply_rocrate_document_with_policy(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
    ) -> Result<Batch> {
        self.apply_rocrate_document_with_policy_and_durability(
            auth,
            graph,
            jsonld,
            policy,
            CraqleRequestDurability::Durable,
        )
    }

    /// Create or replace a visible RO-Crate state with explicit durability.
    pub fn apply_rocrate_document_with_policy_and_durability(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
        durability: CraqleRequestDurability,
    ) -> Result<Batch> {
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager_for_durability(durability)
            .import_jsonld(graph.clone(), jsonld)?;
        self.persist_graph_policy_with_durability(&graph, policy, durability)?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Strict variant of `apply_rocrate_document_with_policy` that validates
    /// complete RO-Crate semantics even for new-graph bootstrap imports.
    pub fn apply_rocrate_document_checked_with_policy(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
    ) -> Result<Batch> {
        self.apply_rocrate_document_checked_with_policy_and_durability(
            auth,
            graph,
            jsonld,
            policy,
            CraqleRequestDurability::Durable,
        )
    }

    /// Strict RO-Crate replacement with explicit request durability.
    pub fn apply_rocrate_document_checked_with_policy_and_durability(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
        durability: CraqleRequestDurability,
    ) -> Result<Batch> {
        self.apply_rocrate_document_checked_with_policy_and_durability_as(
            auth, graph, jsonld, policy, durability, None,
        )
    }

    /// Strict RO-Crate replacement authored under an explicit CRDT actor for
    /// non-publishing writes.
    pub fn apply_rocrate_document_checked_with_policy_and_durability_as(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager_with(durability, actor)
            .import_jsonld_checked(graph.clone(), jsonld)?;
        self.persist_graph_policy_with_durability(&graph, policy, durability)?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Apply a RO-Crate document that was already strictly validated at its
    /// origin, skipping semantic re-validation.
    ///
    /// The event-log payload replicated to this node must be byte-identical to
    /// a document that passed `validate_rocrate_document_checked_with_policy`
    /// (or the checked apply) at the origin. Structural JSON-LD errors are
    /// still rejected; RO-Crate semantic rules are not re-checked. Use the
    /// checked variant for any untrusted input.
    pub fn apply_rocrate_document_prevalidated_with_policy_and_durability_as(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> Result<Batch> {
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager_with(durability, actor)
            .import_jsonld_prevalidated(graph.clone(), jsonld)?;
        self.persist_graph_policy_with_durability(&graph, policy, durability)?;
        self.finish_batch_with_durability(&graph, batch, durability)
    }

    /// Strictly validate and materialize a RO-Crate document without applying it.
    ///
    /// Returns the changes that would be applied, but does not mutate the graph
    /// store, persist policy, enqueue search, or publish Irokle records.
    pub fn validate_rocrate_document_checked_with_policy(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
    ) -> Result<Vec<CoreMaterializedQuadChange>> {
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        Ok(self.manager().plan_import_jsonld_checked(&graph, jsonld)?)
    }

    /// Fast path for trusted RO-Crate bootstrap into a new or empty graph.
    ///
    /// This skips semantic RO-Crate validation and graph diffing, so it should
    /// only be used when the input document is already trusted.
    pub fn bootstrap_rocrate_document(
        &self,
        auth: &dyn Authorizer,
        graph: GraphId,
        jsonld: &str,
        policy: GraphPolicy,
    ) -> Result<Batch> {
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager()
            .bootstrap_jsonld_trusted(graph.clone(), jsonld)?;
        self.persist_graph_policy(&graph, policy)?;
        self.finish_batch(&graph, batch)
    }

    /// Preview the canonical RDF changes implied by a JSON-LD document.
    pub fn preview_rocrate_update(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        jsonld: &str,
    ) -> Result<Vec<CoreMaterializedQuadChange>> {
        if self.store.contains_graph(graph)? {
            self.ensure_graph_action(graph, auth, Action::Write)?;
        }
        Ok(self.manager().plan_import_jsonld(graph, jsonld)?)
    }

    /// Apply a SPARQL update and publish the resulting replication batch.
    pub fn apply_sparql_update(
        &self,
        auth: &dyn Authorizer,
        sparql_update: &str,
    ) -> Result<Option<Batch>> {
        self.apply_sparql_update_with_options(auth, sparql_update, &UpdateOptions::default())
    }

    /// Apply a bounded SPARQL update and publish the resulting replication batch.
    pub fn apply_sparql_update_with_options(
        &self,
        auth: &dyn Authorizer,
        sparql_update: &str,
        options: &UpdateOptions,
    ) -> Result<Option<Batch>> {
        let changes = self.sparql.evaluate_update(auth, sparql_update, options)?;
        if changes.is_empty() {
            return Ok(None);
        }

        let mut authorized = HashSet::new();
        for change in &changes {
            let graph = match change {
                CoreMaterializedQuadChange::Insert { graph, .. }
                | CoreMaterializedQuadChange::Delete { graph, .. } => graph,
            };
            if authorized.insert(graph.clone()) {
                self.ensure_graph_action(graph, auth, Action::Write)?;
            }
        }
        let graph = single_graph_for_changes(&changes)?;
        let batch = self.replication.local_apply_changes(&graph, changes)?;
        Ok(Some(self.finish_batch(&graph, batch)?))
    }

    /// Advanced: insert raw quads directly into one graph.
    pub fn insert_quads(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        quads: Vec<(CoreEncodedTerm, CoreEncodedTerm, CoreEncodedTerm)>,
    ) -> Result<Batch> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let batch = self.replication.local_insert_quads(graph, quads)?;
        self.finish_batch(graph, batch)
    }

    /// Advanced: apply an explicit change set with validation.
    pub fn apply_changes(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        changes: Vec<CoreMaterializedQuadChange>,
    ) -> Result<Batch> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let batch = self.replication.local_apply_changes(graph, changes)?;
        self.finish_batch(graph, batch)
    }

    #[cfg(test)]
    pub(crate) fn apply_changes_bypassing_structural_rules(
        &self,
        graph: &GraphId,
        changes: Vec<CoreMaterializedQuadChange>,
    ) -> Result<Batch> {
        let batch = self
            .replication
            .local_apply_changes_bypassing_structural_rules(graph, changes)?;
        self.finish_batch(graph, batch)
    }

    /// Rebuild graph diagnostics from the current visible graph state.
    pub fn rebuild_graph_diagnostics(&self, graph: &GraphId) -> Result<()> {
        self.replication.rebuild_graph_diagnostics(graph)?;
        Ok(())
    }

    /// Replace or add a single property value on an entity.
    pub fn update_property(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        entity_id: &str,
        predicate: &str,
        old_value: Option<&str>,
        new_value: &str,
    ) -> Result<Batch> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        let batch = self.manager().update_property(
            graph,
            rocrate::PropertyUpdate {
                entity_id,
                predicate,
                old_value,
                new_value,
            },
        )?;
        self.finish_batch(graph, batch)
    }

    /// Execute a SPARQL query against the local node.
    ///
    /// Visibility is decided lazily, once per graph the evaluation touches,
    /// rather than by materializing the whole visible set up front.
    ///
    /// Persisted graph policy is read from the same durable snapshot as query
    /// data. A policy read error or missing graph denies visibility (G8).
    pub fn query(&self, auth: &dyn Authorizer, sparql: &str) -> Result<QueryResults> {
        Ok(self
            .sparql
            .query_with_snapshot_visibility(sparql, &|snapshot, graph: &GraphId| {
                snapshot
                    .graph_policy(&self.store, graph)
                    .ok()
                    .flatten()
                    .is_some_and(|policy| auth.authorize(graph, &policy, Action::Read).is_ok())
            })?)
    }

    /// Parse a SPARQL query for repeated execution.
    ///
    /// The prepared value contains no store snapshot or authorization state;
    /// both are acquired afresh on every execution.
    pub fn prepare_query(&self, sparql: &str) -> Result<PreparedQuery> {
        Ok(self.sparql.prepare_query(sparql)?)
    }

    /// Execute a SPARQL query and return its complete result with diagnostics.
    pub fn query_with_statistics(
        &self,
        auth: &dyn Authorizer,
        sparql: &str,
    ) -> Result<QueryExecution> {
        Ok(self.sparql.query_with_snapshot_visibility_statistics(
            sparql,
            &|snapshot, graph: &GraphId| {
                snapshot
                    .graph_policy(&self.store, graph)
                    .ok()
                    .flatten()
                    .is_some_and(|policy| auth.authorize(graph, &policy, Action::Read).is_ok())
            },
        )?)
    }

    /// Execute a prepared query against a fresh authorized store snapshot.
    pub fn execute_prepared(
        &self,
        auth: &dyn Authorizer,
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryExecution> {
        Ok(self.sparql.execute_prepared_with_snapshot_visibility(
            query,
            &|snapshot, graph: &GraphId| {
                snapshot
                    .graph_policy(&self.store, graph)
                    .ok()
                    .flatten()
                    .is_some_and(|policy| auth.authorize(graph, &policy, Action::Read).is_ok())
            },
            options,
            Duration::ZERO,
            true,
        )?)
    }

    /// Inspect the current logical and physical plan without executing it.
    pub fn explain_prepared(
        &self,
        auth: &dyn Authorizer,
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        Ok(self.sparql.explain_prepared_with_snapshot_visibility(
            query,
            &|snapshot, graph: &GraphId| {
                snapshot
                    .graph_policy(&self.store, graph)
                    .ok()
                    .flatten()
                    .is_some_and(|policy| auth.authorize(graph, &policy, Action::Read).is_ok())
            },
            options,
        )?)
    }

    /// Execute a prepared query completely and return its measured plan.
    pub fn analyze_prepared(
        &self,
        auth: &dyn Authorizer,
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        Ok(self.execute_prepared(auth, query, options)?.statistics.plan)
    }

    /// Execute a SPARQL query against an explicit, wholly authorized graph set.
    ///
    /// Missing and unreadable graph names both fail the complete request with
    /// an authorization error; neither is silently removed from the dataset.
    pub fn query_in_graphs(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        sparql: &str,
    ) -> Result<QueryResults> {
        Ok(self
            .query_in_graphs_with_options(auth, graphs, sparql, &QueryOptions::default())?
            .results)
    }

    /// Execute a complete query over an explicit, wholly authorized graph set.
    pub fn query_in_graphs_with_options(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        sparql: &str,
        options: &QueryOptions,
    ) -> Result<QueryExecution> {
        let query = self.prepare_query(sparql)?;
        self.execute_prepared_in_graphs(auth, graphs, &query, options)
    }

    /// Execute a prepared query over an explicit, wholly authorized graph set.
    pub fn execute_prepared_in_graphs(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryExecution> {
        Ok(self
            .sparql
            .execute_prepared_in_graphs(auth, query, graphs, options)?)
    }

    /// Inspect a prepared plan for an explicit, wholly authorized graph set.
    pub fn explain_prepared_in_graphs(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        Ok(self
            .sparql
            .explain_prepared_in_graphs(auth, query, graphs, options)?)
    }

    /// Execute over explicit authorized graphs and return the measured plan.
    pub fn analyze_prepared_in_graphs(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        query: &PreparedQuery,
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        Ok(self
            .execute_prepared_in_graphs(auth, graphs, query, options)?
            .statistics
            .plan)
    }

    /// Execute a SPARQL query where graph visibility is decided by `visible`.
    ///
    /// The predicate is evaluated lazily over the union view: it runs at most
    /// once per graph the evaluation actually touches (memoized for the
    /// duration of the query), so the cost scales with the graphs a query
    /// reaches instead of the total corpus. A quad participates in evaluation
    /// iff its graph satisfies the predicate; the predicate must be cheap and
    /// side-effect free.
    #[cfg(test)]
    pub(crate) fn query_graphs_with<F>(&self, visible: F, sparql: &str) -> Result<QueryResults>
    where
        F: Fn(&GraphId) -> bool,
    {
        Ok(self.sparql.query_with_visibility(sparql, &visible)?)
    }

    /// Compatibility no-op: durable qv indexes are maintained with graph
    /// commits and source storage remains the fallback authority.
    pub fn ensure_query_indexes(&self) {
        self.store.ensure_derived_indexes();
    }

    /// Search visible resources in the local search index.
    ///
    /// Clamps the limit, authorizes hits against stored policy, and drops
    /// duplicates so each graph-and-subject pair fills at most one page slot.
    pub fn search(&self, auth: &dyn Authorizer, req: SearchRequest<'_>) -> Result<Vec<SearchHit>> {
        let limit = req.limit.min(MAX_SEARCH_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut readable = ReadableGraphs::new(self, auth);
        // Bounded by the clamp above: escalation only widens while the index
        // actually filled the previous fetch, so it tracks the corpus.
        let mut fetch = limit.saturating_mul(4).max(SEARCH_MIN_FETCH);
        loop {
            let raw_hits = self.search.search(req.query, fetch)?;
            // Fewer hits than asked for means the index has nothing more to
            // give; widening again cannot produce another readable hit.
            let index_exhausted = raw_hits.len() < fetch;

            let mut seen = SeenHits::default();
            let mut hits = Vec::with_capacity(raw_hits.len().min(limit));
            for hit in raw_hits {
                if seen.admits(&hit) && readable.allows(&hit.graph_id)? {
                    hits.push(hit);
                }
            }

            if hits.len() >= limit || index_exhausted {
                // Score-descending order arrives from the index and both
                // filters preserve it, so no re-sort is needed here.
                hits.truncate(limit);
                return Ok(hits);
            }
            fetch = fetch.saturating_mul(4);
        }
    }

    /// Search visible resources in an explicit set of graph IRIs.
    ///
    /// `req.limit` is clamped to [`MAX_SEARCH_LIMIT`] (10_000), never rejected.
    ///
    /// Every selected graph is authorized against its stored policy *before*
    /// the index is consulted, so no post-filtering — and therefore no
    /// escalation loop — is needed: every hit the index can return already
    /// belongs to a graph the caller may read. Missing or non-readable graphs
    /// are ignored, matching [`CraqleNode::search`].
    pub fn search_graphs(
        &self,
        auth: &dyn Authorizer,
        req: GraphSearchRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
        // Clamped once here, so both arms and the final ordering agree on it.
        let req = GraphSearchRequest {
            limit: req.limit.min(MAX_SEARCH_LIMIT),
            ..req
        };
        if req.limit == 0 {
            return Ok(Vec::new());
        }

        let mut seen = std::collections::HashSet::new();
        let mut selected = Vec::new();
        for graph in req.graphs {
            if !seen.insert(graph.as_str()) {
                continue;
            }
            if !self.store.contains_graph(graph)?
                || auth
                    .authorize(graph, &self.store.graph_policy(graph)?, Action::Read)
                    .is_err()
            {
                continue;
            }
            selected.push(graph.clone());
        }

        if selected.is_empty() {
            return Ok(Vec::new());
        }

        // A per-graph search is a full top-k collection each, so it only pays
        // off for a handful of graphs; beyond that one filtered search over
        // the whole set is cheaper. This is a performance fork only — the two
        // arms must answer identically, which `graph_arms_agree` pins down.
        let hits = if selected.len() <= SEARCH_GRAPHS_PER_GRAPH_LIMIT {
            self.search_graph_arm(&selected, &req)?
        } else {
            self.search_set_arm(&selected, &req)?
        };

        // Both arms are ordered here rather than in one of them, so a tie
        // cannot resolve differently either side of the threshold.
        Ok(limit_search_hits(hits, req.limit))
    }

    /// One full top-k collection per graph, concatenated for the caller to order.
    fn search_graph_arm(
        &self,
        selected: &[GraphId],
        req: &GraphSearchRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
        let mut hits = Vec::new();
        for graph in selected {
            hits.extend(
                self.search
                    .search_in_graph(graph.as_str(), req.query, req.limit)?,
            );
        }
        Ok(hits)
    }

    /// One collection over the whole set, narrowed by a graph filter.
    fn search_set_arm(
        &self,
        selected: &[GraphId],
        req: &GraphSearchRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
        Ok(self.search.search_in_graphs(search::GraphSetQuery {
            graphs: selected,
            query: req.query,
            limit: req.limit,
        })?)
    }

    /// Resolve one visible subject into `(predicate, object)` pairs.
    pub fn describe_subject(
        &self,
        auth: &dyn Authorizer,
        req: DescribeRequest<'_>,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.ensure_graph_action(req.graph, auth, Action::Read)?;
        let ctx = self.describe_ctx(req.graph)?;
        self.describe_in_ctx(&ctx, req.subject_id)
    }

    /// Hydrate search hits with visible RDF properties.
    ///
    /// Search results usually cluster into a handful of graphs, so the policy
    /// read and the orphan-set rebuild are memoized per graph rather than
    /// repeated per hit. Hits in a graph the caller may not read
    /// are skipped rather than failing the whole call, matching how
    /// [`CraqleNode::search`] drops them.
    pub fn hydrate_search_hits(
        &self,
        auth: &dyn Authorizer,
        hits: &[SearchHit],
    ) -> Result<Vec<HydratedSearchHit>> {
        let mut contexts: HashMap<String, Option<DescribeCtx>> = HashMap::new();
        let mut hydrated = Vec::with_capacity(hits.len());

        for hit in hits {
            let ctx = match contexts.entry(hit.graph_id.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let graph = GraphId::new(&hit.graph_id);
                    let ctx = match self.ensure_graph_action(&graph, auth, Action::Read) {
                        Ok(()) => Some(self.describe_ctx(&graph)?),
                        Err(CraqleError::Authorization(_)) => None,
                        Err(error) => return Err(error),
                    };
                    entry.insert(ctx)
                }
            };

            let Some(ctx) = ctx.as_ref() else {
                continue;
            };
            hydrated.push(HydratedSearchHit {
                hit: hit.clone(),
                properties: self.describe_in_ctx(ctx, &hit.subject_iri)?,
            });
        }

        Ok(hydrated)
    }

    /// Search and hydrate visible resources in one call.
    pub fn search_resources(
        &self,
        auth: &dyn Authorizer,
        req: SearchRequest<'_>,
    ) -> Result<Vec<HydratedSearchHit>> {
        let hits = self.search(auth, req)?;
        self.hydrate_search_hits(auth, &hits)
    }

    /// Block until the background full-text indexer has processed queued work.
    pub fn flush_search_updates(&self) -> Result<()> {
        self.search_worker.flush()
    }

    /// Rebuild the full-text index from store state.
    ///
    /// Commits Tantivy and persists Fjall once per batch of graphs rather than
    /// once per graph: every commit replays the queued deletes against every
    /// segment, which made a per-graph commit super-linear in corpus size.
    pub fn reindex_search(&self) -> Result<()> {
        let mut covered = Vec::with_capacity(REINDEX_COMMIT_BATCH_GRAPHS);
        for graph in self.store.graphs()? {
            // Pinned before the scan reads anything: a write landing later is
            // not covered by it and must outlive the clear below.
            // `current_dirty_token` is the next token to be minted, so the
            // highest one this scan can cover is the one before it.
            let upto = self.store.current_dirty_token().saturating_sub(1);
            self.search.reindex_from_store(&self.store, &graph)?;
            covered.push(ScannedGraph { graph, upto });
            #[cfg(test)]
            self.gate_after_scan();
            if covered.len() >= REINDEX_COMMIT_BATCH_GRAPHS {
                self.commit_reindexed_graphs(&mut covered)?;
            }
        }
        self.commit_reindexed_graphs(&mut covered)
    }

    /// Commit the Tantivy work for `covered`, then clear those graphs' FTS
    /// queue entries, then persist once.
    ///
    /// ORDERING HAZARD (G7): the queue clearing MUST follow the Tantivy commit
    /// that covers these graphs. Clear first and crash before the commit, and
    /// those updates are lost permanently — nothing would ever re-enqueue
    /// them. Crash after the commit but before the clear and the worker merely
    /// re-does the work on its next drain. Only the second direction is safe,
    /// so the order below is not an implementation detail.
    fn commit_reindexed_graphs(&self, covered: &mut Vec<ScannedGraph>) -> Result<()> {
        if covered.is_empty() {
            return Ok(());
        }
        self.search.commit()?;
        for scanned in covered.drain(..) {
            self.store
                .clear_fts_queue_for_graph(&scanned.graph, scanned.upto)?;
        }
        self.persist_fjall()
    }

    /// Hold a reindex between a graph's scan and the clear that covers it,
    /// reporting arrival and waiting for release. Test-only: it makes a window
    /// that is otherwise microseconds wide something a test can step through.
    #[cfg(test)]
    fn gate_after_scan(&self) {
        let gate = self
            .reindex_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(gate) = gate {
            let _ = gate.reached.send(());
            let _ = gate.go.recv_timeout(Duration::from_secs(10));
        }
    }

    /// Arm the gate for the next graph a reindex scans. Test-only; the test
    /// that uses it needs a real index, so the stub build never calls it.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn set_reindex_gate(
        &self,
        reached: mpsc::Sender<()>,
        go: mpsc::Receiver<()>,
    ) -> &Self {
        *self
            .reindex_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ReindexGate { reached, go });
        self
    }

    /// Run manual store compaction as a post-ingest maintenance step.
    pub fn manual_compact_store(&self) -> Result<()> {
        self.store.manual_compact()?;
        Ok(())
    }

    /// Return the lifecycle state of the disposable persistent query indexes.
    pub fn query_index_status(&self) -> Result<QueryIndexStatus> {
        Ok(self.store.query_index_status()?)
    }

    /// Return query-index v2 readiness using metadata and exact counters only.
    pub fn query_index_status_fast(&self) -> Result<QueryIndexStatus> {
        Ok(self.store.query_index_status_fast()?)
    }

    /// Rebuild the disposable persistent query indexes from canonical CRDT quad state.
    pub fn rebuild_query_indexes(&self) -> Result<QueryIndexStatus> {
        self.store.rebuild_query_indexes()?;
        self.persist_fjall()?;
        self.query_index_status_fast()
    }

    /// Diagnose persistent query indexes without changing source or index data.
    pub fn verify_query_indexes(
        &self,
        mode: impl Into<QueryIndexVerificationMode>,
    ) -> Result<QueryIndexVerification> {
        Ok(self.store.verify_query_indexes(mode.into())?)
    }

    #[cfg(test)]
    pub(crate) fn set_graph_policy_bypassing_authorization(
        &self,
        graph: &GraphId,
        policy: GraphPolicy,
    ) -> Result<()> {
        self.validate_sync_policy(graph, &policy)?;
        self.set_local_graph_policy(graph, policy.normalized())?;
        self.persist_fjall()
    }

    /// Every graph the caller may read.
    ///
    /// Streams graph term ids and decodes each name through the shared term
    /// cache instead of materializing the full graph list first. This is O(corpus)
    /// by definition; prefer [`CraqleNode::query`], which checks visibility
    /// only for the graphs a query touches.
    pub fn visible_graphs(&self, auth: &dyn Authorizer) -> Result<Vec<GraphId>> {
        let mut visible = Vec::new();
        for graph_id in self.store.graph_term_id_iter() {
            let term = self.store.decode_term(graph_id?)?;
            let Some(graph) = term.to_named_node().map(GraphId) else {
                continue;
            };
            let policy = self.store.graph_policy(&graph)?;
            if auth.authorize(&graph, &policy, Action::Read).is_ok() {
                visible.push(graph);
            }
        }
        Ok(visible)
    }

    pub fn graphs(&self) -> Result<Vec<GraphId>> {
        Ok(self.store.graphs()?)
    }

    pub fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        Ok(self.store.contains_graph(graph)?)
    }

    pub fn delete_graph(&self, auth: &dyn Authorizer, graph: &GraphId) -> Result<()> {
        self.ensure_graph_action(graph, auth, Action::Write)?;
        self.delete_graph_after_authorization(graph)
    }

    fn delete_graph_after_authorization(&self, graph: &GraphId) -> Result<()> {
        // Orders the tombstone against this graph's writes, and the publish
        // against its own apply; see `replication::GRAPH_WRITE_LOCKS`. Every
        // tombstone writer takes it, so a write that checks the tombstone
        // before applying cannot race one halfway through.
        let _write_guard = replication::graph_write_guard(graph);

        if self.store.graph_tombstoned(graph)? {
            return Ok(());
        }
        let mut delete_clock = self.store.get_vector_clock(graph)?;
        let delete_counter = delete_clock
            .0
            .get(&self.actor)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        delete_clock.advance(self.actor, delete_counter);
        let tombstone = GraphTombstone {
            graph: graph.clone(),
            delete_event: EventId::graph_delete(graph, self.actor, &delete_clock),
            delete_actor: self.actor,
            delete_clock,
        };

        if let Some(sync) = &self.sync
            && sync.graph_topic_id(&self.store, graph)?.is_some()
        {
            let record = sync.publish_delete(&self.store, tombstone)?;
            self.apply_irokle_record_locked(&record, true)?;
            return self.persist_fjall();
        }
        self.store.delete_graph_tombstoned(&tombstone)?;
        self.schedule_search_update();
        self.persist_fjall()
    }

    pub fn vector_clock(&self, graph: &GraphId) -> Result<VectorClock> {
        Ok(self.store.get_vector_clock(graph)?)
    }

    pub fn graph_fingerprint(&self, graph: &GraphId) -> Result<(u64, [u8; 32], [u8; 32])> {
        Ok(self.store.graph_fingerprint(graph)?)
    }

    /// Read-only dump of one graph's quad and dot state, for diagnostics and
    /// test assertions. Not a sync mechanism.
    pub fn graph_snapshot(&self, graph: &GraphId) -> Result<GraphReplicaSnapshot> {
        Ok(self.store.graph_snapshot(graph)?)
    }

    /// Build the per-graph state `describe_in_ctx` needs.
    fn describe_ctx(&self, graph: &GraphId) -> Result<DescribeCtx> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        Ok(DescribeCtx {
            graph_tid: self.store.lookup_term(&graph_term)?,
            orphaned: self.orphaned_entities(graph)?,
        })
    }

    /// Resolve a subject's visible `(predicate, object)` pairs within a graph
    /// whose readability the caller has already established.
    ///
    /// The orphan set is load-bearing twice: it hides orphaned subjects, and it
    /// drops triples whose *object* points at an orphan. Both are required for
    /// G6 ("invalid visible crates are never exported"). Returning an empty
    /// list for an orphaned subject, rather than an error, is deliberate.
    ///
    /// `subject_id` may name a blank node (`_:b0`) — search indexes and returns
    /// them in that form — so it is encoded with `from_subject_id`. Encoding it
    /// as an IRI both let an orphaned blank node through the check below and
    /// made every non-orphaned blank node describe as empty.
    fn describe_in_ctx(
        &self,
        ctx: &DescribeCtx,
        subject_id: &str,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        let subject = EncodedTerm::from_subject_id(subject_id);
        if ctx.orphaned.contains(&subject) {
            return Ok(Vec::new());
        }

        let Some(graph_tid) = ctx.graph_tid else {
            return Ok(Vec::new());
        };
        let Some(subject_tid) = self.store.lookup_term(&subject)? else {
            return Ok(Vec::new());
        };

        Ok(self
            .store
            .triples_for_subject(graph_tid, subject_tid)?
            .into_iter()
            .filter(|(_, object)| !ctx.orphaned.contains(object))
            .collect())
    }

    fn ensure_graph_action(
        &self,
        graph: &GraphId,
        auth: &dyn Authorizer,
        action: Action,
    ) -> Result<()> {
        let policy = self.store.graph_policy(graph)?;
        auth.authorize(graph, &policy, action)?;
        Ok(())
    }

    fn ensure_policy_action(
        &self,
        graph: &GraphId,
        next_policy: &GraphPolicy,
        auth: &dyn Authorizer,
        action: Action,
    ) -> Result<()> {
        let policy = if self.store.contains_graph(graph)? {
            self.store.graph_policy(graph)?
        } else {
            next_policy.clone()
        };
        auth.authorize(graph, &policy, action)?;
        Ok(())
    }

    fn persist_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> Result<()> {
        self.persist_graph_policy_with_durability(graph, policy, CraqleRequestDurability::Durable)
    }

    fn persist_graph_policy_with_durability(
        &self,
        graph: &GraphId,
        policy: GraphPolicy,
        durability: CraqleRequestDurability,
    ) -> Result<()> {
        let _write_guard = replication::graph_write_guard(graph);
        if let Some(tombstone) = self.store.graph_tombstone(graph)? {
            return Err(UpdateError::GraphDeleted { tombstone }.into());
        }
        let previous = self.store.graph_tagged_policy(graph)?;
        let policy = policy.normalized();
        if self.store.contains_graph(graph)? && previous.policy == policy {
            return Ok(());
        }
        let tagged = TaggedGraphPolicy {
            policy,
            tag: PolicyTag::next_local(previous.tag, self.actor),
        };

        if durability.publishes_irokle()
            && let Some(sync) = &self.sync
        {
            let record = sync.publish_policy(&self.store, graph, tagged)?;
            stall_publish_apply();
            self.apply_irokle_record_locked(&record, true)?;
            return Ok(());
        }

        self.store.set_tagged_graph_policy(graph, &tagged)?;
        Ok(())
    }

    #[cfg(test)]
    fn set_local_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> Result<()> {
        self.persist_graph_policy_with_durability(
            graph,
            policy,
            CraqleRequestDurability::WalAlreadyDurable,
        )
    }

    fn orphaned_entities(&self, graph: &GraphId) -> Result<std::collections::HashSet<EncodedTerm>> {
        Ok(self
            .store
            .graph_diagnostics(graph)?
            .orphaned_entities
            .into_iter()
            .map(|entity_id| EncodedTerm::from_subject_id(&entity_id))
            .collect())
    }

    fn finish_batch(&self, graph: &GraphId, batch: Batch) -> Result<Batch> {
        self.finish_batch_with_durability(graph, batch, CraqleRequestDurability::Durable)
    }

    fn finish_batch_with_durability(
        &self,
        graph: &GraphId,
        batch: Batch,
        durability: CraqleRequestDurability,
    ) -> Result<Batch> {
        self.schedule_search_update_for_graph(graph)?;
        if durability.persists_fjall() {
            self.persist_fjall()?;
        }
        Ok(batch)
    }

    fn finish_report(
        &self,
        graph: &GraphId,
        report: AppendDataEntitiesReport,
    ) -> Result<AppendDataEntitiesReport> {
        self.schedule_search_update_for_graph(graph)?;
        self.persist_fjall()?;
        Ok(report)
    }

    fn schedule_full_search_reindex(&self) -> Result<()> {
        let mut batch = self.store.new_batch();
        for graph_id in self.store.graph_term_ids()? {
            self.store.enqueue_fts_reindex(&mut batch, graph_id)?;
        }
        self.store.commit(batch)?;
        self.persist_fjall()?;
        self.schedule_search_update();
        Ok(())
    }

    fn schedule_search_update_for_graph(&self, graph: &GraphId) -> Result<()> {
        if self.store.contains_graph(graph)? {
            self.schedule_search_update();
        }
        Ok(())
    }

    fn schedule_search_update(&self) {
        self.search_worker.wake();
    }

    pub fn persist_fjall(&self) -> Result<()> {
        Ok(self.store.persist()?)
    }

    /// Apply one replicated record to local state.
    ///
    /// Every arm here is a compare-and-set — read the tombstone, the stored
    /// policy or the stored context tag, then decide whether to write — so the
    /// whole record has to be applied under the graph's write lock. Without it
    /// two applies both read the same stored value, both conclude they win, and
    /// the one that lands second decides the outcome by arrival order; for the
    /// `@context` register that means the local value can end up superseded on
    /// every peer but this one, with no later event to correct it (G5, G8).
    #[cfg(test)]
    fn apply_irokle_record(
        &self,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<bool> {
        // Orders this apply against every other write to the same graph; see
        // `replication::GRAPH_WRITE_LOCKS`.
        let _write_guard = replication::graph_write_guard(record.event.graph());
        self.apply_irokle_record_locked(record, false)
    }

    /// **Call with the graph's write lock held**, so a publish and its own
    /// apply cannot be reordered against a concurrent one.
    fn apply_irokle_record_locked(
        &self,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
        local_record: bool,
    ) -> Result<bool> {
        if let CraqleGraphEvent::GraphDeleted { tombstone } = &record.event {
            self.store.delete_graph_tombstoned(tombstone)?;
            self.schedule_search_update();
            return Ok(true);
        }
        if let Some(tombstone) = self.store.graph_tombstone(record.event.graph())? {
            return Err(CraqleError::ReplicationRejected {
                error_kind: CraqleErrorKind::Conflict,
                reason: format!(
                    "graph {} was permanently deleted by event {}",
                    tombstone.graph, tombstone.delete_event
                ),
            });
        }
        match &record.event {
            CraqleGraphEvent::GraphDeleted { .. } => unreachable!(),
            CraqleGraphEvent::Policy { graph, tagged } => {
                let record_actor = ActorId::from_bytes(*record.meta.actor_id.as_bytes());
                if tagged.tag.actor != record_actor {
                    return Err(CraqleError::ReplicationRejected {
                        error_kind: CraqleErrorKind::CorruptAuthoritativeData,
                        reason: "policy tag actor does not match the signed record actor"
                            .to_owned(),
                    });
                }
                if !local_record
                    && !self.remote_policy_authorizer.may_apply_policy(
                        graph,
                        &tagged.tag.actor,
                        &tagged.policy,
                    )
                {
                    return Err(CraqleError::ReplicationRejected {
                        error_kind: CraqleErrorKind::Unauthorized,
                        reason: "remote actor is not authorized to change graph policy".to_owned(),
                    });
                }
                let current = self.store.graph_tagged_policy(graph)?;
                if tagged.tag <= current.tag {
                    return Ok(false);
                }
                let tagged = TaggedGraphPolicy {
                    policy: tagged.policy.clone().normalized(),
                    tag: tagged.tag,
                };
                self.store.set_tagged_graph_policy(graph, &tagged)?;
                Ok(true)
            }
            CraqleGraphEvent::QuadChanges { graph, .. }
            | CraqleGraphEvent::RoCrateMutation { graph, .. } => {
                let Some(result) = self.replication.apply_irokle_record(record)? else {
                    return Ok(false);
                };
                if result.applied {
                    self.schedule_search_update_for_graph(graph)?;
                }
                Ok(result.applied)
            }
        }
    }

    #[cfg(test)]
    fn validate_sync_policy(&self, graph: &GraphId, policy: &GraphPolicy) -> Result<()> {
        if policy.permission_paths.len() > MAX_SYNC_POLICY_PATHS {
            return Err(CraqleError::SyncInputRejected(format!(
                "sync policy for graph `{}` exceeded {} permission paths",
                graph.as_str(),
                MAX_SYNC_POLICY_PATHS
            )));
        }
        Ok(())
    }

    fn manager(&self) -> RoCrateManager {
        RoCrateManager::new(self.replication.clone())
    }

    fn manager_with(
        &self,
        durability: CraqleRequestDurability,
        actor: Option<ActorId>,
    ) -> RoCrateManager {
        match (durability.publishes_irokle(), actor) {
            (true, _) => self.manager(),
            (false, None) => RoCrateManager::new(self.local_replication.clone()),
            (false, Some(actor)) => {
                #[cfg(feature = "shacl-core")]
                let replication = ReplicationEngine::new_with_shacl(
                    self.store.clone(),
                    self.sparql.clone(),
                    actor,
                    self.shacl.clone(),
                );
                #[cfg(not(feature = "shacl-core"))]
                let replication =
                    ReplicationEngine::new(self.store.clone(), self.sparql.clone(), actor);
                RoCrateManager::new(Arc::new(replication))
            }
        }
    }

    fn manager_for_durability(&self, durability: CraqleRequestDurability) -> RoCrateManager {
        self.manager_with(durability, None)
    }
}

#[cfg(feature = "shacl-core")]
fn rocrate_policy_id(shapes_graph: &GraphId, schema: &CompiledShaclSchema) -> PolicyId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"craqle-rocrate-policy-v1");
    hasher.update(&(shapes_graph.as_str().len() as u64).to_be_bytes());
    hasher.update(shapes_graph.as_str().as_bytes());
    hasher.update(&schema.plan_fingerprint());
    hasher.update(&SHACL_COMPILER_MODEL_VERSION.to_be_bytes());
    PolicyId(*hasher.finalize().as_bytes())
}

fn single_graph_for_changes(changes: &[CoreMaterializedQuadChange]) -> Result<GraphId> {
    let Some(first) = changes.first() else {
        return Err(CraqleError::MultiGraphUpdateUnsupported);
    };
    let graph = match first {
        CoreMaterializedQuadChange::Insert { graph, .. }
        | CoreMaterializedQuadChange::Delete { graph, .. } => graph.clone(),
    };

    if changes.iter().all(|change| match change {
        CoreMaterializedQuadChange::Insert {
            graph: change_graph,
            ..
        }
        | CoreMaterializedQuadChange::Delete {
            graph: change_graph,
            ..
        } => *change_graph == graph,
    }) {
        Ok(graph)
    } else {
        Err(CraqleError::MultiGraphUpdateUnsupported)
    }
}

/// Per-graph state for resolving several subjects of the same graph.
///
/// `graph_tid` is `None` when the graph name was never interned, i.e. the
/// graph holds no triples to describe.
struct DescribeCtx {
    graph_tid: Option<store::TermId>,
    orphaned: std::collections::HashSet<EncodedTerm>,
}

/// Memo of "may this caller read this graph?", valid for one call.
///
/// Authorization is always re-evaluated against the policy currently in the
/// store, so a policy change is picked up by the next call (G8).
struct ReadableGraphs<'a> {
    node: &'a CraqleNode,
    auth: &'a dyn Authorizer,
    memo: HashMap<String, bool>,
}

impl<'a> ReadableGraphs<'a> {
    fn new(node: &'a CraqleNode, auth: &'a dyn Authorizer) -> Self {
        Self {
            node,
            auth,
            memo: HashMap::new(),
        }
    }

    fn allows(&mut self, graph_id: &str) -> Result<bool> {
        if let Some(readable) = self.memo.get(graph_id) {
            return Ok(*readable);
        }

        let graph = GraphId::new(graph_id);
        let readable = self.node.store.contains_graph(&graph)?
            && self
                .auth
                .authorize(&graph, &self.node.store.graph_policy(&graph)?, Action::Read)
                .is_ok();
        self.memo.insert(graph_id.to_string(), readable);
        Ok(readable)
    }
}

fn score_key(score: f32) -> i64 {
    (score as f64 * 1_000_000.0) as i64
}

/// Remembers graph-and-subject pairs so later duplicates can be dropped.
#[derive(Default)]
pub(crate) struct SeenHits(std::collections::HashSet<(String, String)>);

impl SeenHits {
    /// True exactly once per pair: only the first occurrence is admitted.
    pub(crate) fn admits(&mut self, hit: &SearchHit) -> bool {
        self.0
            .insert((hit.graph_id.clone(), hit.subject_iri.clone()))
    }
}

/// Merge hits from several searches into one score-ordered page, keeping only
/// the highest-scoring occurrence of each graph-and-subject pair.
fn limit_search_hits(mut hits: Vec<SearchHit>, limit: usize) -> Vec<SearchHit> {
    hits.sort_unstable_by(|left, right| {
        Reverse(score_key(left.score))
            .cmp(&Reverse(score_key(right.score)))
            .then_with(|| left.graph_id.cmp(&right.graph_id))
            .then_with(|| left.subject_iri.cmp(&right.subject_iri))
    });
    let mut seen = SeenHits::default();
    hits.retain(|hit| seen.admits(hit));
    hits.truncate(limit);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ContextTag;

    /// Generous enough that a slow machine never trips it, short enough that a
    /// lock-order regression fails the run instead of hanging it.
    const PROGRESS_TIMEOUT: Duration = Duration::from_secs(180);

    fn writer_auth() -> GrantAuthorizer {
        GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)])
    }

    /// A node that replicates through a real irokle topic, so its own published
    /// records come back to it through `reconcile_irokle`.
    fn sync_node(dir: &tempfile::TempDir) -> Arc<CraqleNode> {
        let irokle = irokle::Irokle::builder().build().unwrap();
        Arc::new(
            CraqleNode::open_with_options(
                dir.path(),
                CraqleOptions::new()
                    .with_search_storage(SearchStorage::Memory)
                    .with_irokle(irokle, CraqleIrokleOptions::new()),
            )
            .unwrap(),
        )
    }

    /// Wait for `count` workers, failing rather than hanging on a lock-order
    /// regression.
    fn await_workers(rx: &mpsc::Receiver<()>, count: usize) {
        for _ in 0..count {
            rx.recv_timeout(PROGRESS_TIMEOUT)
                .expect("a concurrent writer never finished");
        }
    }

    fn crate_request(graph: &GraphId, name: &str) -> CreateCrateRequest {
        CreateCrateRequest::new(
            graph.clone(),
            name,
            "description",
            "2025-01-01",
            None,
            GraphPolicy {
                public: true,
                permission_paths: vec!["/t/x".to_string()],
            },
        )
    }

    #[cfg(feature = "shacl-core")]
    fn shacl_change(
        graph: &GraphId,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> MaterializedQuadChange {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm(format!("<{subject}>")),
            predicate: EncodedTerm(format!("<{predicate}>")),
            object: EncodedTerm(format!("<{object}>")),
        }
    }

    #[cfg(feature = "shacl-core")]
    fn independent_graph_ids(prefix: &str, count: usize) -> Vec<GraphId> {
        let mut used = HashSet::new();
        let mut graphs = Vec::new();
        let mut candidate = 0usize;
        while graphs.len() < count {
            let graph = GraphId::new(&format!("urn:test:{prefix}:data:{candidate}"));
            let shard = (store::hash_term(&EncodedTerm::from_named_node(&graph.0)).0 as usize) % 64;
            if used.insert(shard) {
                graphs.push(graph);
            }
            candidate += 1;
        }
        graphs
    }

    #[cfg(feature = "shacl-core")]
    fn bound_policy_graphs(
        node: &CraqleNode,
        prefix: &str,
        count: usize,
        policy: ShaclWritePolicy,
    ) -> Vec<(GraphId, String)> {
        let data_graphs = independent_graph_ids(prefix, count);
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let sh_node = "http://www.w3.org/ns/shacl#NodeShape";
        let sh_property_shape = "http://www.w3.org/ns/shacl#PropertyShape";
        let sh_target = "http://www.w3.org/ns/shacl#targetNode";
        let sh_property = "http://www.w3.org/ns/shacl#property";
        let sh_path = "http://www.w3.org/ns/shacl#path";
        let sh_min_count = "http://www.w3.org/ns/shacl#minCount";
        let xsd_integer = "http://www.w3.org/2001/XMLSchema#integer";
        let mut result = Vec::new();
        for (index, data) in data_graphs.into_iter().enumerate() {
            let shapes = GraphId::new(&format!("urn:test:{prefix}:shapes:{index}"));
            let focus = format!("urn:test:{prefix}:focus:{index}");
            let shape = format!("urn:test:{prefix}:shape:{index}");
            let property = format!("urn:test:{prefix}:property:{index}");
            node.apply_changes_bypassing_structural_rules(
                &data,
                vec![shacl_change(
                    &data,
                    &focus,
                    "urn:test:unrelated-seed",
                    "urn:test:seed",
                )],
            )
            .unwrap();
            let mut shape_changes = vec![
                shacl_change(&shapes, &shape, rdf_type, sh_node),
                shacl_change(&shapes, &shape, sh_target, &focus),
                shacl_change(&shapes, &shape, sh_property, &property),
                shacl_change(&shapes, &property, rdf_type, sh_property_shape),
                shacl_change(&shapes, &property, sh_path, "urn:test:required-value"),
            ];
            shape_changes.push(MaterializedQuadChange::Insert {
                graph: shapes.clone(),
                subject: EncodedTerm(format!("<{property}>")),
                predicate: EncodedTerm(format!("<{sh_min_count}>")),
                object: EncodedTerm(format!("\"1\"^^<{xsd_integer}>")),
            });
            node.apply_changes_bypassing_structural_rules(&shapes, shape_changes)
                .unwrap();
            node.bind_shacl(
                &AllowAllAuthorizer,
                &ShaclBinding {
                    data_graph: data.clone(),
                    shapes_graph: shapes,
                    policy,
                    validation_options: ShaclBindingOptions::default(),
                },
            )
            .unwrap();
            result.push((data, focus));
        }
        result
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn independent_shacl_writers_validate_concurrently() {
        for count in [1usize, 2, 4, 8, 16] {
            for (label, policy, rejected) in [
                ("disabled", ShaclWritePolicy::Disabled, false),
                ("advisory", ShaclWritePolicy::Advisory, false),
                ("enforce-valid", ShaclWritePolicy::Enforce, false),
                ("enforce-rejected", ShaclWritePolicy::Enforce, true),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let node = Arc::new(
                    CraqleNode::open_with_options(
                        directory.path(),
                        CraqleOptions::new().with_search_storage(SearchStorage::Memory),
                    )
                    .unwrap(),
                );
                let graphs =
                    bound_policy_graphs(&node, &format!("writers:{label}:{count}"), count, policy);
                node.store.set_validation_stall(Duration::from_millis(40));
                let start = Arc::new(std::sync::Barrier::new(count + 1));
                let (tx, rx) = mpsc::channel();
                for (index, (graph, focus)) in graphs.into_iter().enumerate() {
                    let node = Arc::clone(&node);
                    let start = Arc::clone(&start);
                    let tx = tx.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        let predicate = if rejected {
                            "urn:test:unrelated-write"
                        } else {
                            "urn:test:required-value"
                        };
                        let result = node.apply_changes(
                            &AllowAllAuthorizer,
                            &graph,
                            vec![shacl_change(
                                &graph,
                                &focus,
                                predicate,
                                &format!("urn:test:writer-value:{index}"),
                            )],
                        );
                        tx.send(result.is_ok()).unwrap();
                    });
                }
                drop(tx);
                start.wait();
                let results = (0..count)
                    .map(|_| rx.recv_timeout(PROGRESS_TIMEOUT).unwrap())
                    .collect::<Vec<_>>();
                let max_active = node.store.validation_max_active();
                node.store.set_validation_stall(Duration::ZERO);
                assert!(results.iter().all(|accepted| *accepted != rejected));
                if policy == ShaclWritePolicy::Disabled {
                    assert_eq!(max_active, 0);
                } else {
                    assert!(max_active >= count.min(2));
                }
            }
        }
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn same_graph_shacl_writers_remain_serialized() {
        let directory = tempfile::tempdir().unwrap();
        let node = Arc::new(
            CraqleNode::open_with_options(
                directory.path(),
                CraqleOptions::new().with_search_storage(SearchStorage::Memory),
            )
            .unwrap(),
        );
        let (graph, focus) =
            bound_policy_graphs(&node, "same-graph-writers", 1, ShaclWritePolicy::Enforce)
                .pop()
                .unwrap();
        node.store.set_validation_stall(Duration::from_millis(40));
        let start = Arc::new(std::sync::Barrier::new(3));
        let (tx, rx) = mpsc::channel();
        for index in 0..2 {
            let node = Arc::clone(&node);
            let start = Arc::clone(&start);
            let tx = tx.clone();
            let graph = graph.clone();
            let focus = focus.clone();
            std::thread::spawn(move || {
                start.wait();
                let accepted = node
                    .apply_changes(
                        &AllowAllAuthorizer,
                        &graph,
                        vec![shacl_change(
                            &graph,
                            &focus,
                            "urn:test:required-value",
                            &format!("urn:test:same-graph-value:{index}"),
                        )],
                    )
                    .is_ok();
                tx.send(accepted).unwrap();
            });
        }
        drop(tx);
        start.wait();
        assert!(rx.recv_timeout(PROGRESS_TIMEOUT).unwrap());
        assert!(rx.recv_timeout(PROGRESS_TIMEOUT).unwrap());
        let max_active = node.store.validation_max_active();
        node.store.set_validation_stall(Duration::ZERO);
        assert_eq!(max_active, 1);
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn shape_dependency_mutation_during_validation_rechecks_fences() {
        for imported in [false, true] {
            let label = if imported { "import" } else { "root" };
            let directory = tempfile::tempdir().unwrap();
            let node = Arc::new(
                CraqleNode::open_with_options(
                    directory.path(),
                    CraqleOptions::new().with_search_storage(SearchStorage::Memory),
                )
                .unwrap(),
            );
            let data = GraphId::new(&format!("urn:test:shape-race:{label}:data"));
            let root = GraphId::new(&format!("urn:test:shape-race:{label}:root"));
            let dependency = if imported {
                GraphId::new(&format!("urn:test:shape-race:{label}:import"))
            } else {
                root.clone()
            };
            let focus = format!("urn:test:shape-race:{label}:focus");
            let shape = format!("urn:test:shape-race:{label}:shape");
            let property = format!("urn:test:shape-race:{label}:property");
            node.apply_changes_bypassing_structural_rules(
                &data,
                vec![shacl_change(
                    &data,
                    &focus,
                    "urn:test:unrelated-seed",
                    "urn:test:seed",
                )],
            )
            .unwrap();
            let mut shape_changes = vec![
                shacl_change(
                    &dependency,
                    &shape,
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://www.w3.org/ns/shacl#NodeShape",
                ),
                shacl_change(
                    &dependency,
                    &shape,
                    "http://www.w3.org/ns/shacl#targetNode",
                    &focus,
                ),
                shacl_change(
                    &dependency,
                    &shape,
                    "http://www.w3.org/ns/shacl#property",
                    &property,
                ),
                shacl_change(
                    &dependency,
                    &property,
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://www.w3.org/ns/shacl#PropertyShape",
                ),
                shacl_change(
                    &dependency,
                    &property,
                    "http://www.w3.org/ns/shacl#path",
                    "urn:test:required-value",
                ),
            ];
            shape_changes.push(MaterializedQuadChange::Insert {
                graph: dependency.clone(),
                subject: EncodedTerm(format!("<{property}>")),
                predicate: EncodedTerm("<http://www.w3.org/ns/shacl#minCount>".to_owned()),
                object: EncodedTerm("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned()),
            });
            node.apply_changes_bypassing_structural_rules(&dependency, shape_changes)
                .unwrap();
            if imported {
                node.apply_changes_bypassing_structural_rules(
                    &root,
                    vec![shacl_change(
                        &root,
                        "urn:test:shape-race:ontology",
                        "http://www.w3.org/2002/07/owl#imports",
                        dependency.as_str(),
                    )],
                )
                .unwrap();
            }
            node.bind_shacl(
                &AllowAllAuthorizer,
                &ShaclBinding {
                    data_graph: data.clone(),
                    shapes_graph: root,
                    policy: ShaclWritePolicy::Enforce,
                    validation_options: ShaclBindingOptions {
                        allow_local_imports: imported,
                        ..ShaclBindingOptions::default()
                    },
                },
            )
            .unwrap();

            node.store.set_validation_stall(Duration::from_millis(100));
            let (tx, rx) = mpsc::channel();
            let writer = Arc::clone(&node);
            let writer_data = data.clone();
            let writer_focus = focus.clone();
            std::thread::spawn(move || {
                let accepted = writer
                    .apply_changes(
                        &AllowAllAuthorizer,
                        &writer_data,
                        vec![shacl_change(
                            &writer_data,
                            &writer_focus,
                            "urn:test:required-value",
                            "urn:test:shape-race:value",
                        )],
                    )
                    .is_ok();
                tx.send(accepted).unwrap();
            });
            let wait_started = Instant::now();
            while node.store.validation_active() == 0 {
                assert!(wait_started.elapsed() < Duration::from_secs(5));
                std::thread::yield_now();
            }
            node.apply_changes_bypassing_structural_rules(
                &dependency,
                vec![MaterializedQuadChange::Insert {
                    graph: dependency.clone(),
                    subject: EncodedTerm(format!("<{property}>")),
                    predicate: EncodedTerm("<http://www.w3.org/ns/shacl#maxCount>".to_owned()),
                    object: EncodedTerm(
                        "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned(),
                    ),
                }],
            )
            .unwrap();
            assert!(!rx.recv_timeout(PROGRESS_TIMEOUT).unwrap());
            node.store.set_validation_stall(Duration::ZERO);
        }
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn sync_local_settlement_failure_returns_committed_batch() {
        let directory = tempfile::tempdir().unwrap();
        let node = sync_node(&directory);
        let (data, focus) =
            bound_policy_graphs(&node, "sync-settlement", 1, ShaclWritePolicy::Enforce)
                .pop()
                .unwrap();
        node.replication.arm_settle_failure();
        let batch = node
            .apply_changes(
                &AllowAllAuthorizer,
                &data,
                vec![shacl_change(
                    &data,
                    &focus,
                    "urn:test:required-value",
                    "urn:test:sync-settlement:value",
                )],
            )
            .unwrap();
        assert_eq!(batch.graph, data);
        assert_eq!(
            node.shacl_binding_statuses(&AllowAllAuthorizer, &data)
                .unwrap()[0]
                .state,
            ShaclValidationState::Pending
        );
        assert_eq!(node.pending_shacl_queue_status().unwrap().pending_count, 1);
        assert_eq!(
            node.pending_shacl_queue_status()
                .unwrap()
                .settlement_failures,
            1
        );
        node.persist_fjall().unwrap();
        drop(node);

        let reopened = CraqleNode::open_with_options(
            directory.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        assert_eq!(
            reopened.startup_pending_replay().statistics.graphs_settled,
            1
        );
        assert_eq!(
            reopened
                .shacl_binding_statuses(&AllowAllAuthorizer, &data)
                .unwrap()[0]
                .state,
            ShaclValidationState::Valid
        );
    }

    #[test]
    fn query_authorization_uses_the_data_snapshot_policy() {
        let directory = tempfile::tempdir().unwrap();
        let node = Arc::new(
            CraqleNode::open_with_options(
                directory.path(),
                CraqleOptions::new().with_search_storage(SearchStorage::Memory),
            )
            .unwrap(),
        );
        let first = GraphId::new("urn:test:query-policy-snapshot:first");
        let second = GraphId::new("urn:test:query-policy-snapshot:second");
        let private = GraphPolicy {
            public: false,
            permission_paths: Vec::new(),
        };
        for graph in [&first, &second] {
            node.store.set_graph_policy(graph, &private).unwrap();
            seed_write(&node, graph);
        }

        let query = format!(
            "ASK {{ GRAPH <{first}> {{ <{first}> <http://schema.org/keywords> \"race\" }} \
             GRAPH <{second}> {{ <{second}> <http://schema.org/keywords> \"race\" }} }}",
            first = first.as_str(),
            second = second.as_str(),
        );
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let node_for_query = Arc::clone(&node);
        let query_thread = std::thread::spawn(move || {
            let first_call = AtomicBool::new(true);
            let release_rx = std::sync::Mutex::new(release_rx);
            let auth = move |graph: &GraphId, policy: &GraphPolicy, action: Action| {
                if first_call.swap(false, Ordering::SeqCst) {
                    reached_tx.send(graph.clone()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(PROGRESS_TIMEOUT)
                        .expect("query authorization was never released");
                    return Ok(());
                }
                if policy.public {
                    Ok(())
                } else {
                    Err(AuthorizationError::PermissionDenied {
                        action,
                        graph: graph.as_str().to_owned(),
                    })
                }
            };
            node_for_query.query(&auth, &query).unwrap()
        });

        let reached = reached_rx
            .recv_timeout(PROGRESS_TIMEOUT)
            .expect("query never reached graph authorization");
        let changed = if reached == first {
            second
        } else {
            assert_eq!(reached, second);
            first
        };
        let term = EncodedTerm::from_named_node(&changed.0);
        let before = node.store.lookup_term(&term).unwrap().unwrap();
        node.store.delete_graph(&changed).unwrap();
        node.store.create_graph(&changed).unwrap();
        node.store
            .set_graph_policy(
                &changed,
                &GraphPolicy {
                    public: true,
                    permission_paths: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(before, node.store.lookup_term(&term).unwrap().unwrap());
        release_tx.send(()).unwrap();

        assert!(matches!(
            query_thread.join().unwrap(),
            QueryResults::Boolean(false)
        ));
    }

    #[cfg(feature = "search")]
    #[test]
    fn query_fts_reauthorizes_hits_after_search() {
        let directory = tempfile::tempdir().unwrap();
        let node = Arc::new(
            CraqleNode::open_with_options(
                directory.path(),
                CraqleOptions::new().with_search_storage(SearchStorage::Memory),
            )
            .unwrap(),
        );
        let graph = GraphId::new("urn:test:query-fts-policy");
        node.store.create_graph(&graph).unwrap();
        node.store
            .set_graph_policy(
                &graph,
                &GraphPolicy {
                    public: true,
                    permission_paths: Vec::new(),
                },
            )
            .unwrap();

        let query = format!(
            "SELECT ?s WHERE {{ SERVICE <urn:craqle:fts> {{ \
             ?s fts:query \"secret\" ; fts:graph <{graph}> ; fts:limit 1 . }} }}",
            graph = graph.as_str(),
        );
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let node_for_query = Arc::clone(&node);
        let query_thread = std::thread::spawn(move || {
            let first_call = AtomicBool::new(true);
            let release_rx = std::sync::Mutex::new(release_rx);
            let auth = move |graph: &GraphId, policy: &GraphPolicy, action: Action| {
                if first_call.swap(false, Ordering::SeqCst) {
                    reached_tx.send(()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(PROGRESS_TIMEOUT)
                        .expect("query authorization was never released");
                    return Ok(());
                }
                if policy.public {
                    Ok(())
                } else {
                    Err(AuthorizationError::PermissionDenied {
                        action,
                        graph: graph.as_str().to_owned(),
                    })
                }
            };
            node_for_query.query(&auth, &query).unwrap()
        });

        reached_rx
            .recv_timeout(PROGRESS_TIMEOUT)
            .expect("query never reached FTS graph authorization");
        node.store
            .set_graph_policy(
                &graph,
                &GraphPolicy {
                    public: false,
                    permission_paths: Vec::new(),
                },
            )
            .unwrap();
        node.search
            .index_resource(graph.as_str(), "urn:test:secret", Some("secret"))
            .unwrap();
        node.search.commit().unwrap();
        release_tx.send(()).unwrap();

        assert!(matches!(
            query_thread.join().unwrap(),
            QueryResults::Solutions(rows) if rows.is_empty()
        ));
    }

    /// Both node-level search entry points take a caller-supplied limit that
    /// Tantivy turns into a `limit * 2` pre-allocation, so `usize::MAX` used
    /// to abort the process rather than return a page.
    #[test]
    #[cfg(feature = "search")]
    fn huge_limit_clamps() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth = writer_auth();

        let graph = GraphId::new("urn:test:huge-limit");
        node.create_crate(&auth, crate_request(&graph, "hugeneedle"))
            .unwrap();
        node.flush_search_updates().unwrap();

        let hits = node
            .search(
                &auth,
                SearchRequest {
                    query: "hugeneedle",
                    limit: usize::MAX,
                },
            )
            .unwrap();
        assert_eq!(1, hits.len());

        let hits = node
            .search_graphs(
                &auth,
                GraphSearchRequest {
                    graphs: std::slice::from_ref(&graph),
                    query: "hugeneedle",
                    limit: usize::MAX,
                },
            )
            .unwrap();
        assert_eq!(1, hits.len());
    }

    /// `search_graphs` forks strategy above `SEARCH_GRAPHS_PER_GRAPH_LIMIT`
    /// graphs. That is a performance fork, so both arms must answer
    /// identically — including for a query carrying characters the Tantivy
    /// parser reads as syntax, which the set arm used to hand over unescaped.
    #[test]
    #[cfg(feature = "search")]
    fn graph_arms_agree() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth = writer_auth();

        // Only the first seven graphs carry the needle, so querying seven
        // graphs and querying all nine must return the same hits — one query
        // either side of the threshold.
        let graphs: Vec<GraphId> = (0..9)
            .map(|i| GraphId::new(&format!("urn:t:arm{i}")))
            .collect();
        for (index, graph) in graphs.iter().enumerate() {
            let name = if index < 7 {
                "agreeneedle"
            } else {
                "quiettext"
            };
            node.create_crate(&auth, crate_request(graph, name))
                .unwrap();
        }
        node.flush_search_updates().unwrap();

        let run = |set: &[GraphId]| {
            node.search_graphs(
                &auth,
                GraphSearchRequest {
                    graphs: set,
                    query: "agreeneedle:one",
                    limit: 20,
                },
            )
            .expect("both arms must accept the same query")
            .into_iter()
            .map(|hit| (hit.graph_id, hit.subject_iri))
            .collect::<Vec<_>>()
        };

        let per_graph = run(&graphs[..7]);
        assert_eq!(7, per_graph.len(), "the needle must be findable at all");
        assert_eq!(per_graph, run(&graphs), "the two arms disagree");
    }

    /// A whole-graph rebuild clears the graph, then refills it from a store
    /// scan. An upsert for the same graph landing in that window survived the
    /// clear and was then duplicated by the refill, leaving two documents for
    /// one subject that the acknowledged queue entries called settled (G7).
    #[test]
    #[cfg(feature = "search")]
    fn rebuild_excludes_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth = writer_auth();

        let graph = GraphId::new("urn:test:rebuild-upsert-race");
        node.create_crate(&auth, crate_request(&graph, "racyneedle"))
            .unwrap();
        node.flush_search_updates().expect("baseline flush");

        node.search
            .arm_rebuild_stall(std::time::Duration::from_millis(300));
        let rebuild = {
            let (search, store, graph) = (node.search.clone(), node.store.clone(), graph.clone());
            std::thread::spawn(move || search.reindex_from_store(&store, &graph).unwrap())
        };

        // Re-index the same subject with the text the refill is about to write,
        // so a lost race shows up as two documents rather than one.
        node.search.await_rebuild_stall();
        node.search
            .index_resource(graph.as_str(), graph.as_str(), Some("racyneedle"))
            .unwrap();
        rebuild.join().expect("the rebuild thread panicked");
        node.search.commit().unwrap();

        assert_eq!(
            1,
            node.search.search("racyneedle", 10).unwrap().len(),
            "the upsert and the refill each produced a document for one subject"
        );
    }

    /// Seeds two searchable subjects, then plants a second index document for
    /// the strongest one so its subject appears twice in raw rankings.
    #[cfg(feature = "search")]
    fn duplicated_node(dir: &tempfile::TempDir) -> (CraqleNode, GraphId, GraphId) {
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth = writer_auth();

        let strong = GraphId::new("urn:test:dedup-strong");
        let weak = GraphId::new("urn:test:dedup-weak");
        node.create_crate(&auth, crate_request(&strong, "quiettext"))
            .unwrap();
        node.create_crate(&auth, crate_request(&weak, "dupneedle"))
            .unwrap();
        node.flush_search_updates().unwrap();

        node.search
            .index_resource(
                strong.as_str(),
                "urn:dupneedle:dupneedle",
                Some("dupneedle"),
            )
            .unwrap();
        node.search
            .seed_duplicate(strong.as_str(), "urn:dupneedle:dupneedle")
            .unwrap();
        node.search.commit().unwrap();
        (node, strong, weak)
    }

    /// A duplicated index document must not consume a page slot: both search
    /// entry points must fill a two-slot page with two distinct subjects.
    #[test]
    #[cfg(feature = "search")]
    fn search_dedups_subjects() {
        let dir = tempfile::tempdir().unwrap();
        let (node, strong, weak) = duplicated_node(&dir);
        let auth = writer_auth();

        let hits = node
            .search(
                &auth,
                SearchRequest {
                    query: "dupneedle",
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(2, hits.len());
        assert_ne!(
            hits[0].subject_iri, hits[1].subject_iri,
            "one subject filled both page slots"
        );

        let graphs = [strong, weak];
        let hits = node
            .search_graphs(
                &auth,
                GraphSearchRequest {
                    graphs: &graphs,
                    query: "dupneedle",
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(2, hits.len());
        assert_ne!(
            hits[0].subject_iri, hits[1].subject_iri,
            "one subject filled both page slots"
        );
    }

    /// Duplicate index documents must not become duplicate rows: the FTS
    /// clause binds each matching subject exactly once.
    #[test]
    #[cfg(feature = "search")]
    fn fts_dedups_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (node, _strong, _weak) = duplicated_node(&dir);

        let sparql = r#"
            SELECT ?s ?g
            WHERE {
                SERVICE <urn:craqle:fts> {
                    ?s fts:query "dupneedle" .
                    ?s fts:graph ?g .
                    ?s fts:limit 10 .
                }
            }
        "#;
        let rows = match node.query_graphs_with(|_| true, sparql).unwrap() {
            QueryResults::Solutions(rows) => rows,
            other => panic!("expected solutions, got {other:?}"),
        };
        assert_eq!(2, rows.len(), "duplicate hits changed row cardinality");
        let subjects: std::collections::HashSet<_> = rows
            .iter()
            .map(|row| row.get("s").expect("subject must be bound").0.clone())
            .collect();
        assert_eq!(2, subjects.len(), "a subject was bound more than once");
    }

    /// Two replicas of one topic: `origin` publishes, `replica` picks the
    /// records up through `reconcile_irokle` into its own store.
    struct ReplicaPair {
        _dir: tempfile::TempDir,
        irokle: irokle::Irokle,
        origin: CraqleNode,
        replica: CraqleNode,
    }

    fn replica_pair() -> ReplicaPair {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let open = |name: &str| {
            CraqleNode::open_with_options(
                dir.path().join(name),
                CraqleOptions::new()
                    .with_search_storage(SearchStorage::Memory)
                    .with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
            )
            .unwrap()
        };
        ReplicaPair {
            origin: open("origin"),
            replica: open("replica"),
            irokle,
            _dir: dir,
        }
    }

    /// Options carrying a sync handle the caller keeps, so a test can arm a
    /// failing history read on the node it is about to open.
    fn armed_sync<S: irokle::Storage>(
        node: irokle::Irokle<S>,
    ) -> (Arc<IrokleGraphSync<S>>, CraqleOptions) {
        let sync = Arc::new(IrokleGraphSync::new(node, CraqleIrokleOptions::new()));
        let mut options = CraqleOptions::new().with_search_storage(SearchStorage::Memory);
        options.sync = Some(sync.clone());
        (sync, options)
    }

    fn keyword_object(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    /// One keyword write on the crate root, published as a single record.
    fn write_keyword(node: &CraqleNode, graph: &GraphId, value: &str) {
        node.insert_quads(
            &AllowAllAuthorizer,
            graph,
            vec![(
                EncodedTerm::from_named_node(&graph.0),
                EncodedTerm::from_named_node(&vocab::schema_keywords()),
                keyword_object(value),
            )],
        )
        .unwrap();
    }

    fn has_keyword(node: &CraqleNode, graph: &GraphId, value: &str) -> bool {
        let object = keyword_object(value);
        node.graph_snapshot(graph)
            .unwrap()
            .quads
            .iter()
            .any(|quad| quad.object == object)
    }

    fn topic_cursor(node: &CraqleNode, topic: irokle::TopicId) -> Option<Vec<u8>> {
        node.store.applied_topic_clock(topic.as_bytes()).unwrap()
    }

    /// Reopen a replica's store from disk without the reconcile pass `open`
    /// runs, so the persisted cursor can be read before anything advances it.
    fn reopen_replica(dir: &Path, irokle: &irokle::Irokle) -> CraqleNode {
        let store = Arc::new(GraphStore::open(dir.join("store")).unwrap());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        CraqleNode::from_store_and_search(
            store,
            search,
            CraqleOptions::new()
                .with_search_storage(SearchStorage::Memory)
                .with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
    }

    /// A pass must name exactly the graphs it changed, so a caller can
    /// invalidate derived state per graph instead of wholesale.
    #[test]
    fn reconcile_names_graphs() {
        let pair = replica_pair();
        let first = GraphId::new("urn:test:reconcile-names-first");
        let second = GraphId::new("urn:test:reconcile-names-second");
        for graph in [&first, &second] {
            pair.origin
                .create_crate(&writer_auth(), crate_request(graph, "names"))
                .unwrap();
        }

        let applied = pair.replica.reconcile_irokle().unwrap();
        assert_eq!(
            HashSet::from([first.clone(), second.clone()]),
            applied,
            "both created graphs must be reported"
        );

        write_keyword(&pair.origin, &first, "only-first");
        let applied = pair.replica.reconcile_irokle().unwrap();
        assert_eq!(
            HashSet::from([first.clone()]),
            applied,
            "a graph nothing arrived for must not be reported"
        );

        assert!(
            pair.replica.reconcile_irokle().unwrap().is_empty(),
            "a pass with nothing outstanding must report no graphs"
        );
    }

    /// A pass that applies a record and then stalls owes that prefix the
    /// configured durability, and must resume at the record that failed.
    #[test]
    fn stall_persists_prefix() {
        let ReplicaPair {
            _dir,
            irokle,
            origin,
            replica,
        } = replica_pair();
        let graph = GraphId::new("urn:test:reconcile-prefix");
        origin
            .create_crate(&writer_auth(), crate_request(&graph, "prefix"))
            .unwrap();
        replica.reconcile_irokle().unwrap();
        replica.flush_search_updates().unwrap();
        let topic = origin.irokle_topic_id(&graph).unwrap().unwrap();
        let baseline = topic_cursor(&replica, topic);

        // A policy record applies without reaching the injected apply failure,
        // so the quad record behind it is the one that stalls the pass.
        origin
            .set_graph_policy(
                &writer_auth(),
                &graph,
                GraphPolicy {
                    public: false,
                    permission_paths: vec!["/t/x".to_string()],
                },
            )
            .unwrap();
        write_keyword(&origin, &graph, "behind-stall");

        let persists = replica.store.persists();
        replica.replication.arm_apply_failure();
        let error = replica.reconcile_irokle().unwrap_err();
        assert!(
            matches!(error, CraqleError::Merge(MergeError::Store(_))),
            "the stall must reach the caller, got `{error}`"
        );
        assert!(
            !replica.replication.take_apply_failure(),
            "the injected failure never fired, so this test proves nothing"
        );
        assert!(
            replica.store.persists() > persists,
            "the applied prefix must be persisted before the stall returns"
        );
        let stalled = topic_cursor(&replica, topic);
        assert_ne!(
            baseline, stalled,
            "the cursor must cover the applied prefix"
        );

        drop(replica);
        let reopened = reopen_replica(&_dir.path().join("replica"), &irokle);
        assert!(
            !reopened.graph_policy(&graph).unwrap().public,
            "the record applied before the stall must survive the reopen"
        );
        assert_eq!(
            stalled,
            topic_cursor(&reopened, topic),
            "the cursor must survive the reopen"
        );
        assert!(
            !has_keyword(&reopened, &graph, "behind-stall"),
            "the record that failed must still be pending"
        );

        reopened.reconcile_irokle().unwrap();
        assert!(has_keyword(&reopened, &graph, "behind-stall"));
        assert_eq!(
            origin.graph_fingerprint(&graph).unwrap(),
            reopened.graph_fingerprint(&graph).unwrap(),
            "the replicas must converge once the failure is gone"
        );
    }

    /// G3 — a retryable apply failure must stop the pass at that record, and a
    /// later pass must still deliver it.
    ///
    /// Quarantining it instead loses it twice over: the cursor moves past it,
    /// and the record behind it raises the graph clock past its dot, so the
    /// dedup gate would drop it even on redelivery. The replica then stays
    /// short one write forever, with nothing left to repair it.
    #[test]
    fn stall_retries_record() {
        let pair = replica_pair();
        let graph = GraphId::new("urn:test:reconcile-stall");
        pair.origin
            .create_crate(&writer_auth(), crate_request(&graph, "stall"))
            .unwrap();
        pair.replica.reconcile_irokle().unwrap();

        // Two writes by one actor: the second is what makes a skipped first
        // permanent, by raising the clock past its dot.
        write_keyword(&pair.origin, &graph, "first");
        write_keyword(&pair.origin, &graph, "second");

        let topic = pair.origin.irokle_topic_id(&graph).unwrap().unwrap();
        let cursor = topic_cursor(&pair.replica, topic);

        pair.replica.replication.arm_apply_failure();
        let error = pair.replica.reconcile_irokle().unwrap_err();
        assert!(
            matches!(error, CraqleError::Merge(MergeError::Store(_))),
            "the stall must reach the caller, got `{error}`"
        );
        assert!(
            !pair.replica.replication.take_apply_failure(),
            "the injected failure never fired, so this test proves nothing"
        );
        assert!(!has_keyword(&pair.replica, &graph, "first"));
        assert!(
            !has_keyword(&pair.replica, &graph, "second"),
            "a record behind the stalled one must not apply"
        );
        assert_eq!(
            cursor,
            topic_cursor(&pair.replica, topic),
            "the cursor must stay at the record that failed"
        );

        pair.replica.reconcile_irokle().unwrap();
        assert!(has_keyword(&pair.replica, &graph, "first"));
        assert!(has_keyword(&pair.replica, &graph, "second"));
        assert_eq!(
            pair.origin.graph_fingerprint(&graph).unwrap(),
            pair.replica.graph_fingerprint(&graph).unwrap(),
            "the replicas must converge once the failure is gone"
        );
    }

    /// A topic whose history cannot be read must stall, not be skipped: a
    /// silent skip leaves the replica short every record it never saw.
    #[test]
    fn unreadable_topic_stalls() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let origin = CraqleNode::open_with_options(
            dir.path().join("origin"),
            CraqleOptions::new()
                .with_search_storage(SearchStorage::Memory)
                .with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:unreadable-topic");
        origin
            .create_crate(&writer_auth(), crate_request(&graph, "unreadable"))
            .unwrap();

        let (sync, options) = armed_sync(irokle);
        let replica = CraqleNode::open_with_options(dir.path().join("replica"), options).unwrap();

        sync.arm_history_failure();
        let error = replica.reconcile_irokle().unwrap_err();
        assert!(
            matches!(error, CraqleError::Sync(_)),
            "an unreadable topic must reach the caller, got `{error}`"
        );
        assert!(
            !sync.take_history_failure(),
            "the injected failure never fired, so this test proves nothing"
        );

        replica.reconcile_irokle().unwrap();
        assert!(
            replica.contains_graph(&graph).unwrap(),
            "the next pass must still deliver the topic"
        );
    }

    #[test]
    fn corrupt_topic_cursor() {
        let pair = replica_pair();
        let graph = GraphId::new("urn:test:corrupt-topic-cursor");
        pair.origin
            .create_crate(&writer_auth(), crate_request(&graph, "cursor"))
            .unwrap();
        pair.replica.reconcile_irokle().unwrap();
        let topic = pair.origin.irokle_topic_id(&graph).unwrap().unwrap();

        let corrupt = vec![0x81, 0x02, 0x03];
        pair.replica
            .store
            .set_applied_topic_clock(topic.as_bytes(), &corrupt)
            .unwrap();
        let error = pair.replica.reconcile_irokle().unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::CorruptAuthoritativeData);
        assert_eq!(
            pair.replica
                .store
                .applied_topic_clock(topic.as_bytes())
                .unwrap(),
            Some(corrupt.clone()),
            "a corrupt cursor must never be reset or advanced"
        );

        let wrong_digest = [9; 32];
        let error = pair
            .replica
            .repair_irokle_topic_cursor(
                &AllowAllAuthorizer,
                topic,
                wrong_digest,
                irokle::ActorClock::default(),
            )
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::Conflict);

        let audit = pair
            .replica
            .repair_irokle_topic_cursor(
                &AllowAllAuthorizer,
                topic,
                topic_cursor_digest(&corrupt),
                irokle::ActorClock::default(),
            )
            .unwrap();
        assert_eq!(audit.old_cursor_digest, topic_cursor_digest(&corrupt));
        pair.replica.reconcile_irokle().unwrap();
        assert!(pair.replica.contains_graph(&graph).unwrap());
    }

    /// An open must not fail on a reconcile a retry clears, or a node whose
    /// peer blipped once would refuse to start at all.
    #[test]
    fn open_retries_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let origin = CraqleNode::open_with_options(
            dir.path().join("origin"),
            CraqleOptions::new()
                .with_search_storage(SearchStorage::Memory)
                .with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:open-retry");
        origin
            .create_crate(&writer_auth(), crate_request(&graph, "retry"))
            .unwrap();

        let (sync, options) = armed_sync(irokle);
        sync.arm_history_failure();
        let replica = CraqleNode::open_with_options(dir.path().join("replica"), options)
            .expect("a reconcile a retry clears must not fail the open");
        assert!(
            !sync.take_history_failure(),
            "the injected failure never fired, so this test proves nothing"
        );
        assert!(
            replica.contains_graph(&graph).unwrap(),
            "the retry must have caught the node up"
        );
    }

    /// A term too large for the store is content, not weather: it must be
    /// quarantined at the decode boundary rather than poison-pill its topic.
    #[test]
    fn oversize_term_quarantines() {
        let pair = replica_pair();
        let graph = GraphId::new("urn:test:reconcile-oversize");
        pair.origin
            .create_crate(&writer_auth(), crate_request(&graph, "oversize"))
            .unwrap();
        let topic = pair.origin.irokle_topic_id(&graph).unwrap().unwrap();

        let oversize = format!("\"{}\"", "x".repeat(sync::MAX_TERM_BYTES));
        pair.irokle
            .open_topic::<CraqleGraphEvent>(topic)
            .unwrap()
            .publish(CraqleGraphEvent::QuadChanges {
                graph: graph.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: EncodedTerm(oversize),
                }],
            })
            .unwrap();
        write_keyword(&pair.origin, &graph, "behind-oversize");

        pair.replica.reconcile_irokle().unwrap();
        assert!(
            has_keyword(&pair.replica, &graph, "behind-oversize"),
            "an oversized term must not hold back the records behind it"
        );
        assert_eq!(
            pair.origin.graph_fingerprint(&graph).unwrap(),
            pair.replica.graph_fingerprint(&graph).unwrap(),
            "both replicas must reject the same record and converge"
        );
    }

    /// A record no retry could ever accept stays quarantined: the pass skips
    /// it and still applies the records behind it.
    #[test]
    fn rejection_ledger_before_cursor_advance() {
        let ReplicaPair {
            _dir,
            irokle,
            origin,
            replica,
        } = replica_pair();
        let graph = GraphId::new("urn:test:reconcile-rejection");
        origin
            .create_crate(&writer_auth(), crate_request(&graph, "rejection"))
            .unwrap();
        replica.reconcile_irokle().unwrap();
        let topic = origin.irokle_topic_id(&graph).unwrap().unwrap();
        let baseline_cursor = topic_cursor(&replica, topic);

        // A change naming a graph its own event does not: rejected on decode,
        // every time it is offered.
        let elsewhere = GraphId::new("urn:test:reconcile-elsewhere");
        let rejected = irokle
            .open_topic::<CraqleGraphEvent>(topic)
            .unwrap()
            .publish(CraqleGraphEvent::QuadChanges {
                graph: graph.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: elsewhere.clone(),
                    subject: EncodedTerm::from_named_node(&elsewhere.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: keyword_object("injected"),
                }],
            })
            .unwrap();
        write_keyword(&origin, &graph, "after");

        replica.store.arm_commit_failure();
        assert!(replica.reconcile_irokle().is_err());
        assert_eq!(baseline_cursor, topic_cursor(&replica, topic));
        assert!(replica.store.replication_rejections().unwrap().is_empty());

        replica.reconcile_irokle().unwrap();
        assert!(
            has_keyword(&replica, &graph, "after"),
            "a quarantined record must not hold back the ones behind it"
        );
        assert!(!replica.contains_graph(&elsewhere).unwrap());
        let records = replica
            .list_rejected_replication_records(&AllowAllAuthorizer)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, rejected.meta.op_id);
        assert_eq!(records[0].seen_count, 1);

        let retry = replica.retry_rejected_replication_record(
            &AllowAllAuthorizer,
            topic,
            rejected.meta.op_id,
        );
        assert!(retry.is_err(), "retry must repeat normal validation");
        assert_eq!(
            replica
                .inspect_rejected_replication_record(
                    &AllowAllAuthorizer,
                    topic,
                    rejected.meta.op_id,
                )
                .unwrap()
                .unwrap()
                .seen_count,
            2
        );

        drop(replica);
        let reopened = reopen_replica(&_dir.path().join("replica"), &irokle);
        assert_eq!(
            reopened
                .list_rejected_replication_records(&AllowAllAuthorizer)
                .unwrap()
                .len(),
            1,
            "the rejection ledger must survive restart"
        );
        assert!(
            reopened
                .acknowledge_rejected_replication_record(
                    &AllowAllAuthorizer,
                    topic,
                    rejected.meta.op_id,
                )
                .unwrap()
        );
        assert!(
            reopened
                .inspect_rejected_replication_record(
                    &AllowAllAuthorizer,
                    topic,
                    rejected.meta.op_id,
                )
                .unwrap()
                .unwrap()
                .acknowledged
        );
        assert!(
            reopened
                .delete_rejected_replication_record(
                    &AllowAllAuthorizer,
                    topic,
                    rejected.meta.op_id,
                )
                .unwrap()
        );
    }

    /// A panic inside the indexer drain must not take the worker thread down.
    ///
    /// The search index is derived state, and the thread that repairs it is the
    /// same one that drains the queue. If a panic killed it, the index would
    /// stay diverged from the store until the process restarted — the lingering
    /// inconsistency the recovery rules forbid.
    #[test]
    #[cfg(feature = "search")]
    fn worker_survives_panic() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth = writer_auth();

        let first = GraphId::new("urn:test:worker-panic-a");
        node.create_crate(&auth, crate_request(&first, "zebrafish alpha"))
            .unwrap();
        node.flush_search_updates().expect("baseline flush");

        node.search.arm_drain_panic();

        let second = GraphId::new("urn:test:worker-panic-b");
        node.create_crate(&auth, crate_request(&second, "zebrafish beta"))
            .unwrap();

        // The armed cycle surfaces the panic as an error rather than dying; a
        // later cycle then completes normally on the same thread.
        let _ = node.flush_search_updates();
        node.flush_search_updates()
            .expect("worker must still be alive after the panic");
        assert!(
            !node.search.take_armed_drain_panic(),
            "the injected panic must actually have fired, or this test is vacuous"
        );

        let hits = node
            .search(
                &auth,
                SearchRequest {
                    query: "zebrafish",
                    limit: 10,
                },
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            2,
            "both crates must be searchable after the worker panicked"
        );
    }

    /// G5 — two `@context` writes must never mint the same last-write-wins tag.
    ///
    /// The tag is `stored_counter + 1`, so an unsynchronised mint hands the
    /// identical `(counter, actor)` to two different context values: a tie the
    /// register cannot break, which leaves peers free to disagree forever.
    #[test]
    fn context_tags_distinct() {
        const WRITERS: usize = 8;
        const WRITES: usize = 8;

        let dir = tempfile::tempdir().unwrap();
        let node = sync_node(&dir);
        let graph = GraphId::new("urn:test:context-mint");
        let (tx, rx) = mpsc::channel();

        for writer in 0..WRITERS {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            let tx = tx.clone();
            std::thread::spawn(move || {
                for round in 0..WRITES {
                    node.replication
                        .set_graph_context(
                            &graph,
                            Some(format!("ctx-{writer}-{round}")),
                            None,
                            None,
                        )
                        .unwrap();
                }
                tx.send(()).unwrap();
            });
        }
        drop(tx);
        await_workers(&rx, WRITERS);

        assert_eq!(
            (WRITERS * WRITES) as u64,
            node.store.graph_context_tag(&graph).unwrap().counter,
            "every mint must have read a tag no concurrent mint was still using"
        );
    }

    /// G5 — two RO-Crate render-hint mutations racing on one graph must converge on
    /// the higher tag, not on whichever landed last.
    ///
    /// The apply is a compare-and-set: read the stored tag, decide, write. Run
    /// unsynchronised, both applies read the same stored tag, both conclude
    /// they dominate it, and arrival order picks the winner — so the register
    /// can settle on a value every peer has already superseded, with no later
    /// event to correct it.
    #[test]
    fn context_applies_converge() {
        const ROUNDS: usize = 32;

        let dir = tempfile::tempdir().unwrap();
        let node = sync_node(&dir);
        let sync = node.sync.clone().expect("sync node");
        let (tx, rx) = mpsc::channel();

        for round in 0..ROUNDS {
            let graph = context_race_graph(round);
            // Published, not applied: the local register is still at genesis, so
            // both records dominate it and neither apply short-circuits.
            let records = [("low", 1), ("high", 2)].map(|(value, counter)| {
                let tag = ContextTag {
                    counter,
                    actor: node.actor(),
                };
                sync.publish_rocrate_mutation(
                    &node.store,
                    &graph,
                    Vec::new(),
                    crate::core::TaggedRoCrateRenderHints {
                        hints: crate::core::RoCrateRenderHints {
                            context: Some(value.to_string()),
                            license: None,
                            license_digest: None,
                        },
                        tag,
                    },
                )
                .unwrap()
            });

            let start = Arc::new(std::sync::Barrier::new(records.len()));
            for record in records {
                let node = Arc::clone(&node);
                let start = Arc::clone(&start);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    start.wait();
                    node.apply_irokle_record(&record).unwrap();
                    tx.send(()).unwrap();
                });
            }
        }
        drop(tx);
        await_workers(&rx, ROUNDS * 2);

        for round in 0..ROUNDS {
            assert_eq!(
                Some("high".to_string()),
                node.store
                    .graph_context(&context_race_graph(round))
                    .unwrap(),
                "round {round} kept the superseded context"
            );
        }
    }

    fn context_race_graph(round: usize) -> GraphId {
        GraphId::new(&format!("urn:test:context-apply-{round}"))
    }

    fn policy_at(path: &str) -> GraphPolicy {
        GraphPolicy {
            public: true,
            permission_paths: vec![format!("/t/{path}")],
        }
    }

    /// G8 — concurrent policy writes must leave this node on the policy their
    /// publish sequence ends with, which is the one every peer converges to.
    ///
    /// A policy event carries no ordering tag, so publish order is the only
    /// thing that decides the winner. Publishing and applying without a lock
    /// between them lets two writes apply in the opposite order, leaving this
    /// node on a policy its peers have already replaced — the permissive one,
    /// if that is the one that lost.
    #[test]
    fn policy_writes_settle() {
        const ROUNDS: usize = 10;
        const WRITERS: usize = 4;
        const STALL_MICROS: u64 = 2_000;

        let dir = tempfile::tempdir().unwrap();
        let node = sync_node(&dir);

        // Widen the publish→apply window, so a writer that does not hold it
        // open under a lock is overtaken instead of merely being able to be.
        PUBLISH_APPLY_STALL_MICROS.store(STALL_MICROS, Ordering::Relaxed);
        for round in 0..ROUNDS {
            let graph = GraphId::new(&format!("urn:test:policy-race-{round}"));
            node.set_graph_policy_bypassing_authorization(&graph, policy_at("seed"))
                .unwrap();

            let (tx, rx) = mpsc::channel();
            for writer in 0..WRITERS {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let tx = tx.clone();
                std::thread::spawn(move || {
                    node.set_graph_policy(&writer_auth(), &graph, policy_at(&format!("w{writer}")))
                        .unwrap();
                    tx.send(()).unwrap();
                });
            }
            drop(tx);
            await_workers(&rx, WRITERS);

            assert_eq!(
                last_published_policy(&node, &graph),
                node.store.graph_policy(&graph).unwrap(),
                "round {round} settled on a policy its peers have replaced"
            );
        }
        PUBLISH_APPLY_STALL_MICROS.store(0, Ordering::Relaxed);
    }

    /// The policy a peer replaying this graph's topic ends up on.
    fn last_published_policy(node: &CraqleNode, graph: &GraphId) -> GraphPolicy {
        let sync = node.sync.clone().expect("sync node");
        let topic = sync
            .graph_topic_id(&node.store, graph)
            .unwrap()
            .expect("a bound topic");
        sync.topic_records_since(topic, None)
            .unwrap()
            .records
            .into_iter()
            .filter_map(|record| match record {
                sync::TopicRecord::Event(record) => match record.event {
                    CraqleGraphEvent::Policy { tagged, .. } => Some(tagged.policy.normalized()),
                    _ => None,
                },
                sync::TopicRecord::Rejected(_) => None,
            })
            .next_back()
            .expect("at least one published policy")
    }

    /// G4 — a write racing a delete must not resurrect the graph.
    ///
    /// The local write applies through the replication engine, which never
    /// passes `CraqleNode::apply_irokle_record`'s tombstone check; without one
    /// of its own it re-creates the graph the delete just tombstoned. Nothing
    /// clears a tombstone, so every later replicated record for that graph is
    /// dropped and the divergence can never be repaired.
    #[test]
    fn write_never_resurrects() {
        const ROUNDS: usize = 16;

        let dir = tempfile::tempdir().unwrap();
        let node = sync_node(&dir);
        let (tx, rx) = mpsc::channel();

        for round in 0..ROUNDS {
            let graph = GraphId::new(&format!("urn:test:delete-race-{round}"));
            node.set_graph_policy_bypassing_authorization(&graph, policy_at("delete-race"))
                .unwrap();
            seed_write(&node, &graph);

            let start = Arc::new(std::sync::Barrier::new(2));
            for racer in 0..2 {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let start = Arc::clone(&start);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    start.wait();
                    if racer == 0 {
                        node.delete_graph_after_authorization(&graph).unwrap();
                    } else {
                        if let Err(error) = seed_write_result(&node, &graph) {
                            assert_eq!(error.kind(), CraqleErrorKind::Conflict);
                        }
                    }
                    tx.send(()).unwrap();
                });
            }
        }
        drop(tx);
        await_workers(&rx, ROUNDS * 2);

        for round in 0..ROUNDS {
            let graph = GraphId::new(&format!("urn:test:delete-race-{round}"));
            assert!(
                node.store.graph_tombstoned(&graph).unwrap(),
                "round {round} never recorded the delete"
            );
            assert!(
                !node.contains_graph(&graph).unwrap(),
                "round {round} resurrected a tombstoned graph"
            );
        }
    }

    #[test]
    fn local_write_after_delete_and_same_id_recreation_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:local-write-after-delete");
        node.create_crate(&writer_auth(), crate_request(&graph, "deleted"))
            .unwrap();
        node.delete_graph(&writer_auth(), &graph).unwrap();
        let tombstone = node.store.graph_tombstone(&graph).unwrap().unwrap();

        let error = seed_write_result(&node, &graph).unwrap_err();
        assert!(matches!(
            error,
            CraqleError::Update(UpdateError::GraphDeleted { .. })
        ));
        let error = node
            .create_crate(&writer_auth(), crate_request(&graph, "recreated"))
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::Conflict);
        assert!(!node.contains_graph(&graph).unwrap());
        assert_eq!(node.store.graph_tombstone(&graph).unwrap(), Some(tombstone));
    }

    #[test]
    fn post_delete_remote_record_rejection() {
        let pair = replica_pair();
        let graph = GraphId::new("urn:test:post-delete-remote-record");
        pair.origin
            .create_crate(&writer_auth(), crate_request(&graph, "deleted"))
            .unwrap();
        pair.replica.reconcile_irokle().unwrap();
        let topic = pair.origin.irokle_topic_id(&graph).unwrap().unwrap();
        pair.origin.delete_graph(&writer_auth(), &graph).unwrap();
        pair.replica.reconcile_irokle().unwrap();

        let rejected = pair
            .irokle
            .open_topic::<CraqleGraphEvent>(topic)
            .unwrap()
            .publish(CraqleGraphEvent::QuadChanges {
                graph: graph.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: keyword_object("late"),
                }],
            })
            .unwrap();

        pair.replica.reconcile_irokle().unwrap();
        let records = pair
            .replica
            .list_rejected_replication_records(&AllowAllAuthorizer)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, rejected.meta.op_id);
        assert_eq!(records[0].error_kind, CraqleErrorKind::Conflict);
        assert!(!pair.replica.contains_graph(&graph).unwrap());
        assert_eq!(
            seed_write_result(&pair.replica, &graph).unwrap_err().kind(),
            CraqleErrorKind::Conflict,
            "a partitioned writer must reject new local writes after learning the tombstone"
        );
        assert_eq!(
            pair.origin.store.graph_tombstone(&graph).unwrap(),
            pair.replica.store.graph_tombstone(&graph).unwrap()
        );
    }

    #[test]
    fn three_replica_delete_write_arrival_order_permutations_converge() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = sync_node(&source_dir);
        let graph = GraphId::new("urn:test:delete-arrival-permutations");
        let sync = source.sync.clone().unwrap();
        let change = |value: &str| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&graph.0),
            predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
            object: keyword_object(value),
        };
        let first = sync
            .publish_changes(&source.store, &graph, vec![change("first")])
            .unwrap();
        let second = sync
            .publish_changes(&source.store, &graph, vec![change("second")])
            .unwrap();
        let mut delete_clock = VectorClock::default();
        delete_clock.advance(source.actor(), 1);
        let tombstone = GraphTombstone {
            graph: graph.clone(),
            delete_event: EventId::graph_delete(&graph, source.actor(), &delete_clock),
            delete_actor: source.actor(),
            delete_clock,
        };
        let deleted = sync
            .publish_delete(&source.store, tombstone.clone())
            .unwrap();

        let target_root = tempfile::tempdir().unwrap();
        let targets: Vec<_> = (0..3)
            .map(|index| {
                CraqleNode::open_with_options(
                    target_root.path().join(index.to_string()),
                    CraqleOptions::new().with_search_storage(SearchStorage::Memory),
                )
                .unwrap()
            })
            .collect();
        let permutations = [
            [&first, &second, &deleted],
            [&deleted, &first, &second],
            [&second, &deleted, &first],
        ];
        for (target, records) in targets.iter().zip(permutations) {
            for record in records {
                if let Err(error) = target.apply_irokle_record(record) {
                    assert_eq!(error.kind(), CraqleErrorKind::Conflict);
                }
            }
            assert!(!target.contains_graph(&graph).unwrap());
            assert_eq!(
                target.store.graph_tombstone(&graph).unwrap(),
                Some(tombstone.clone())
            );
        }
        assert_eq!(
            targets[0].graph_fingerprint(&graph).unwrap(),
            targets[1].graph_fingerprint(&graph).unwrap()
        );
        assert_eq!(
            targets[1].graph_fingerprint(&graph).unwrap(),
            targets[2].graph_fingerprint(&graph).unwrap()
        );
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn delete_replays() {
        let pair = replica_pair();
        let data = GraphId::new("urn:test:delete-replay-data");
        let shapes = GraphId::new("urn:test:delete-replay-shapes");
        let focus = EncodedTerm("<urn:test:delete-replay-focus>".to_owned());
        pair.origin
            .apply_changes_bypassing_structural_rules(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: focus.clone(),
                    predicate: EncodedTerm("<urn:test:delete-replay-value>".to_owned()),
                    object: EncodedTerm("<urn:test:delete-replay-object>".to_owned()),
                }],
            )
            .unwrap();
        pair.origin
            .apply_changes_bypassing_structural_rules(
                &shapes,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: EncodedTerm("<urn:test:delete-replay-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm("<http://www.w3.org/ns/shacl#NodeShape>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: EncodedTerm("<urn:test:delete-replay-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#targetNode>".to_owned(),
                        ),
                        object: focus,
                    },
                ],
            )
            .unwrap();
        pair.replica.reconcile_irokle().unwrap();
        pair.replica
            .bind_shacl(
                &AllowAllAuthorizer,
                &ShaclBinding {
                    data_graph: data.clone(),
                    shapes_graph: shapes.clone(),
                    policy: ShaclWritePolicy::Advisory,
                    validation_options: ShaclBindingOptions::default(),
                },
            )
            .unwrap();

        pair.origin
            .delete_graph_after_authorization(&shapes)
            .unwrap();
        pair.replica.store.set_graph_tombstone(&shapes).unwrap();
        pair.replica.store.arm_commit_failure();
        assert!(pair.replica.reconcile_irokle().is_err());
        assert!(pair.replica.contains_graph(&shapes).unwrap());

        pair.replica.reconcile_irokle().unwrap();
        assert!(pair.replica.store.graph_tombstoned(&shapes).unwrap());
        assert!(!pair.replica.contains_graph(&shapes).unwrap());
        assert_eq!(
            pair.replica.store.shacl_binding_statuses(&data).unwrap()[0].state,
            ShaclValidationState::Pending
        );
        assert_eq!(
            pair.replica.store.pending_shacl_graphs().unwrap(),
            vec![data]
        );
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn binding_status_reads_scale_with_records_not_shape_triples() {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            directory.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let data = GraphId::new("urn:test:status-cost:data");
        node.store.create_graph(&data).unwrap();
        let data_version = node.store.graph_version_digest(&data).unwrap();
        let mut previous = 0usize;

        for count in [1usize, 10, 100, 1_000] {
            let mut graph_batch = node.store.new_batch();
            for index in previous..count {
                node.store
                    .stage_graph(
                        &mut graph_batch,
                        &GraphId::new(&format!("urn:test:status-cost:shapes:{index}")),
                    )
                    .unwrap();
            }
            node.store.commit(graph_batch).unwrap();

            let mut binding_batch = node.store.new_batch();
            for index in previous..count {
                let shapes = GraphId::new(&format!("urn:test:status-cost:shapes:{index}"));
                let shapes_version = node.store.graph_version_digest(&shapes).unwrap();
                node.store
                    .stage_binding_status(
                        &mut binding_batch,
                        &ShaclBindingStatus {
                            binding: ShaclBinding {
                                data_graph: data.clone(),
                                shapes_graph: shapes.clone(),
                                policy: ShaclWritePolicy::Advisory,
                                validation_options: ShaclBindingOptions::default(),
                            },
                            state: ShaclValidationState::Valid,
                            report: Some(ShaclValidationReport {
                                conforms: true,
                                accepted_by_write_policy: true,
                                results: Vec::new(),
                                statistics: ShaclValidationStatistics::default(),
                            }),
                            error: None,
                            data_version,
                            shapes_version,
                            schema_fingerprint: [index as u8; 32],
                            compiler_model_version: SHACL_COMPILER_MODEL_VERSION,
                            shape_versions: vec![(shapes, shapes_version)],
                        },
                    )
                    .unwrap();
            }
            node.store.commit(binding_batch).unwrap();

            let before = node.shacl_runtime_statistics();
            let statuses = node
                .shacl_binding_statuses(&AllowAllAuthorizer, &data)
                .unwrap();
            let after = node.shacl_runtime_statistics();
            assert_eq!(statuses.len(), count);
            assert!(statuses.iter().all(|status| {
                status.state == ShaclValidationState::Valid && status.report.is_some()
            }));
            assert_eq!(
                after.status_bindings_read - before.status_bindings_read,
                count as u64
            );
            assert_eq!(
                after.status_version_checks - before.status_version_checks,
                (count * 2) as u64
            );
            assert_eq!(
                after.status_shape_compilations,
                before.status_shape_compilations
            );
            assert_eq!(
                after.status_full_shape_scans,
                before.status_full_shape_scans
            );
            previous = count;
        }
    }

    /// A bare quad write, skipping the crate-structure rules these tests are
    /// not about.
    fn seed_write(node: &CraqleNode, graph: &GraphId) {
        seed_write_result(node, graph).unwrap();
    }

    fn seed_write_result(node: &CraqleNode, graph: &GraphId) -> Result<Batch> {
        node.apply_changes_bypassing_structural_rules(
            graph,
            vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: EncodedTerm("\"race\"".to_string()),
            }],
        )
    }
}
