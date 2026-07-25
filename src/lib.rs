//! Craqle stores, validates, queries, searches, and replicates RO-Crates.
//!
//! For application integration, prefer the root `craqle` API centered around
//! [`CraqleNode`], typed request structs, and RO-Crate JSON-LD import/export.
//! The lower-level modules exposed from `src/internal/` remain available for
//! advanced use cases and tests, but they are not the primary integration
//! surface.

#[path = "internal/core.rs"]
mod core;
#[path = "internal/planner.rs"]
mod planner;
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
#[path = "internal/sparql.rs"]
mod sparql;
#[path = "internal/store.rs"]
mod store;

mod auth;
mod sync;

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::panic;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use crate::core::{
    EncodedTerm as CoreEncodedTerm, MaterializedQuadChange as CoreMaterializedQuadChange,
};
use crate::replication::ReplicationEngine;
use crate::rocrate::RoCrateManager;
use crate::search::SearchIndex;
use crate::sparql::SparqlEngine;
use crate::store::GraphStore;
use oxrdf::{NamedNode, Term};

pub use crate::core::{
    ActorId, Batch, CrateViolation, EncodedTerm, GraphDiagnostics, GraphId, GraphPolicy,
    MaterializedQuadChange, PredicateFilter, VectorClock, vocab,
};
pub use crate::core::{Dot, GraphReplicaSnapshot, QuadOp, SnapshotQuadState};
pub use crate::replication::{MergeError, UpdateError};
pub use crate::rocrate::{
    AppendDataEntitiesReport, CanonicalJsonLd, NewDataEntity, RoCrateError, RoCratePage,
    canonicalize_jsonld, validate_rocrate_jsonld,
};
pub use crate::search::SearchHit;
pub use crate::sparql::QueryResults;
pub use crate::sync::{CraqleGraphEvent, CraqleIrokleOptions, CraqleSyncError, IrokleGraphSync};
pub use auth::{
    Action, AllowAllAuthorizer, AuthorizationError, Authorizer, DenyAllAuthorizer, GrantAuthorizer,
    PermissionGrant, PermissionLevel,
};
pub use irokle;

#[derive(Debug, thiserror::Error)]
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
    Sparql(#[from] sparql::SparqlError),
    #[error("update: {0}")]
    Update(#[from] replication::UpdateError),
    #[error("merge: {0}")]
    Merge(#[from] replication::MergeError),
    #[error("rocrate: {0}")]
    RoCrate(#[from] rocrate::RoCrateError),
    #[error("sync input rejected: {0}")]
    SyncInputRejected(String),
    #[error("sync: {0}")]
    Sync(#[from] sync::CraqleSyncError),
    #[error("search worker: {0}")]
    SearchWorker(String),
    #[error("unsupported update across multiple graphs")]
    MultiGraphUpdateUnsupported,
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

impl From<CraqleFjallPersistMode> for fjall::PersistMode {
    fn from(mode: CraqleFjallPersistMode) -> Self {
        match mode {
            CraqleFjallPersistMode::Buffer => Self::Buffer,
            CraqleFjallPersistMode::SyncData => Self::SyncData,
            CraqleFjallPersistMode::SyncAll => Self::SyncAll,
        }
    }
}

impl From<fjall::PersistMode> for CraqleFjallPersistMode {
    fn from(mode: fjall::PersistMode) -> Self {
        match mode {
            fjall::PersistMode::Buffer => Self::Buffer,
            fjall::PersistMode::SyncData => Self::SyncData,
            fjall::PersistMode::SyncAll => Self::SyncAll,
        }
    }
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

/// Input for updating one property value on an existing entity.
#[derive(Debug, Clone)]
pub struct UpdatePropertyRequest {
    pub graph: GraphId,
    pub entity_id: String,
    pub predicate: String,
    pub old_value: Option<String>,
    pub new_value: String,
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
    /// one unbounded-channel send per write (finding W15c).
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

/// Drain the FTS queues until everything enqueued *before this call* is indexed.
///
/// The bound matters: without it, a writer that keeps enqueueing between the
/// drain and the acknowledgement keeps the loop alive forever and
/// `flush_search_updates()` never returns (finding W15b). Pinning the dirty
/// token up front turns the contract into "everything enqueued before the call
/// is indexed", which is what callers actually need, and lets sustained ingest
/// carry on in the background.
/// Runs one drain cycle, turning a panic into an error instead of letting it
/// kill the indexer thread.
///
/// The search index is derived state whose only source of truth is the store. A
/// panic here would poison the Tantivy writer mutex *and* take the one thread
/// able to repair it, so the index would stay diverged from the store until the
/// process restarted — the lingering inconsistency the recovery rules forbid.
/// Catching it keeps the loop alive so the next cycle's poisoned-writer recovery
/// rebuilds the writer and re-derives the index. The caller already backs off
/// before retrying, so a persistently panicking drain cannot spin hot.
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
        let processed = search.process_queued_updates_bounded(store, bound)?;
        if processed == 0 {
            if processed_any {
                store.persist()?;
            }
            return Ok(());
        }
        processed_any = true;
    }
}

/// Main application handle for local RO-Crate operations.
///
/// Prefer this root API for service integration. It offers authorization-aware
/// RO-Crate creation, entity append/update operations, JSON-LD export, search,
/// and replication message handling without requiring direct access to the
/// lower-level storage or replication internals.
pub struct CraqleNode {
    actor: ActorId,
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
    search_worker: SearchUpdateWorker,
    _index_warmer: DerivedIndexWarmer,
    sparql: Arc<SparqlEngine>,
    replication: Arc<ReplicationEngine>,
    local_replication: Arc<ReplicationEngine>,
    sync: Option<Arc<dyn sync::CraqleGraphSync>>,
}

// Joined on drop so the store (and its fjall lock) cannot outlive the node.
struct DerivedIndexWarmer {
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DerivedIndexWarmer {
    fn start(store: &Arc<GraphStore>) -> Self {
        let store = Arc::downgrade(store);
        Self {
            handle: Some(std::thread::spawn(move || {
                if let Some(store) = store.upgrade() {
                    store.ensure_derived_indexes();
                }
            })),
        }
    }
}

impl Drop for DerivedIndexWarmer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Configuration used when constructing a [`CraqleNode`].
pub struct CraqleOptions {
    actor: ActorId,
    sync: Option<Arc<dyn sync::CraqleGraphSync>>,
    search_storage: SearchStorage,
    graph_store_persist_mode: CraqleFjallPersistMode,
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
            search_storage: SearchStorage::default(),
            graph_store_persist_mode: CraqleFjallPersistMode::default(),
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

    pub fn with_irokle<S: irokle::Storage>(
        mut self,
        node: irokle::Irokle<S>,
        options: CraqleIrokleOptions,
    ) -> Self {
        self.sync = Some(Arc::new(IrokleGraphSync::new(node, options)));
        self
    }

    fn into_parts(self) -> (ActorId, Option<Arc<dyn sync::CraqleGraphSync>>) {
        (self.actor, self.sync)
    }
}

impl CraqleNode {
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

        let store = Arc::new(GraphStore::open_with_persist_mode(
            root.join("store"),
            graph_store_persist_mode.into(),
        )?);
        let search = Arc::new(match search_storage {
            SearchStorage::Disk => SearchIndex::open(root.join("search"))?,
            SearchStorage::Memory => SearchIndex::open_in_memory()?,
        });
        let search_needs_rebuild =
            search.needs_rebuild() || search_storage == SearchStorage::Memory;
        let node = Self::from_store_and_search(store, search.clone(), options);
        node.reconcile_irokle()?;
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
        let (actor, sync) = options.into_parts();
        // Cross-graph derived indexes are built off the boot path so the
        // first multi-graph query does not pay the build under a write lock.
        let index_warmer = DerivedIndexWarmer::start(&store);
        let search_worker = SearchUpdateWorker::start(store.clone(), search.clone());
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search.clone()));
        let local_replication =
            Arc::new(ReplicationEngine::new(store.clone(), sparql.clone(), actor));
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
            _index_warmer: index_warmer,
            sparql,
            replication,
            local_replication,
            sync,
        }
    }

    /// Return the local actor id used for authored replication batches.
    pub fn actor(&self) -> ActorId {
        self.actor
    }

    /// Return the Fjall persistence mode used for explicit graph-store persists.
    pub fn graph_store_persist_mode(&self) -> CraqleFjallPersistMode {
        self.store.persist_mode().into()
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

    pub fn reconcile_irokle(&self) -> Result<usize> {
        let Some(sync) = &self.sync else {
            return Ok(0);
        };

        let mut applied = 0;
        for topic_id in sync.craqle_topic_ids()? {
            applied += self.reconcile_irokle_topic(sync, topic_id)?;
        }
        if applied > 0 {
            self.persist_fjall()?;
        }
        Ok(applied)
    }

    fn reconcile_irokle_topic(
        &self,
        sync: &Arc<dyn sync::CraqleGraphSync>,
        topic_id: irokle::TopicId,
    ) -> Result<usize> {
        let stored_cursor = self.store.applied_topic_clock(topic_id.as_bytes())?;
        let catchup = match sync.topic_records_since(topic_id, stored_cursor.as_deref()) {
            Ok(catchup) => catchup,
            Err(error) => {
                tracing::warn!(
                    topic = %topic_id,
                    %error,
                    "skipping unreadable craqle topic during reconcile",
                );
                return Ok(0);
            }
        };

        let mut applied = 0;
        for record in &catchup.records {
            match self.apply_reconciled_record(sync, topic_id, record) {
                Ok(true) => applied += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        topic = %topic_id,
                        %error,
                        "quarantined craqle record during reconcile",
                    );
                }
            }
        }
        if let Some(cursor) = catchup.cursor {
            self.store
                .set_applied_topic_clock(topic_id.as_bytes(), &cursor)?;
        }
        Ok(applied)
    }

    fn apply_reconciled_record(
        &self,
        sync: &Arc<dyn sync::CraqleGraphSync>,
        topic_id: irokle::TopicId,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<bool> {
        let graph = record.event.graph();
        match self.store.topic_graph_binding(topic_id.as_bytes())? {
            Some(bound) if bound != graph.as_str() => {
                tracing::warn!(
                    topic = %topic_id,
                    bound = %bound,
                    claimed = %graph.as_str(),
                    "rejected craqle record targeting a graph outside its topic binding",
                );
                return Ok(false);
            }
            Some(_) => {}
            None => sync.bind_graph_topic(&self.store, graph, topic_id)?,
        }
        self.apply_irokle_record(record)
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

    /// Create a new RO-Crate graph.
    pub fn create_crate(
        &self,
        auth: &dyn Authorizer,
        request: CreateCrateRequest,
    ) -> Result<Batch> {
        self.create_crate_with_durability(auth, request, CraqleRequestDurability::Durable)
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
        let batch = self.manager_with(durability, actor).create_crate(
            graph.clone(),
            &name,
            &description,
            &date_published,
            license.as_deref(),
        )?;
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

    /// Export a paged JSON-LD view using an entity-id cursor.
    pub fn export_rocrate_page_after(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        after_entity_id: Option<&str>,
        limit: usize,
    ) -> Result<RoCratePage> {
        self.ensure_graph_action(graph, auth, Action::Read)?;
        Ok(self
            .manager()
            .export_jsonld_page_after(graph, after_entity_id, limit)?)
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
        let changes = self.sparql.evaluate_update(sparql_update)?;
        if changes.is_empty() {
            return Ok(None);
        }

        let graph = single_graph_for_changes(&changes)?;
        self.ensure_graph_action(&graph, auth, Action::Write)?;
        let batch = self.replication.local_apply_changes(&graph, changes)?;
        Ok(Some(self.finish_batch(&graph, batch)?))
    }

    /// Advanced: apply a SPARQL update locally without authorization checks.
    pub fn local_update(&self, sparql_update: &str) -> Result<Option<Batch>> {
        let batch = self.replication.local_update(sparql_update)?;
        if let Some(batch) = batch {
            let graph = batch.graph.clone();
            return Ok(Some(self.finish_batch(&graph, batch)?));
        }
        Ok(None)
    }

    /// Alias for [`CraqleNode::local_update`].
    pub fn update(&self, sparql_update: &str) -> Result<Option<Batch>> {
        self.local_update(sparql_update)
    }

    /// Advanced: insert raw quads directly into one graph.
    pub fn insert_quads(
        &self,
        graph: &GraphId,
        quads: Vec<(CoreEncodedTerm, CoreEncodedTerm, CoreEncodedTerm)>,
    ) -> Result<Batch> {
        let batch = self.replication.local_insert_quads(graph, quads)?;
        self.finish_batch(graph, batch)
    }

    /// Advanced: apply an explicit change set with validation.
    pub fn apply_changes(
        &self,
        graph: &GraphId,
        changes: Vec<CoreMaterializedQuadChange>,
    ) -> Result<Batch> {
        let batch = self.replication.local_apply_changes(graph, changes)?;
        self.finish_batch(graph, batch)
    }

    /// Advanced: apply an explicit change set without post-state validation.
    pub fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<CoreMaterializedQuadChange>,
    ) -> Result<Batch> {
        let batch = self
            .replication
            .local_apply_changes_unchecked(graph, changes)?;
        self.finish_batch(graph, batch)
    }

    /// Advanced: apply a large change set using the bulk write path.
    pub fn apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<CoreMaterializedQuadChange>,
    ) -> Result<Batch> {
        let batch = self
            .replication
            .local_apply_changes_bulk_unchecked(graph, changes)?;
        self.finish_batch(graph, batch)
    }

    /// Rebuild graph diagnostics from the current visible graph state.
    pub fn rebuild_graph_diagnostics(&self, graph: &GraphId) -> Result<()> {
        self.replication.rebuild_graph_diagnostics(graph)?;
        Ok(())
    }

    /// Update a property using a typed request.
    pub fn update_property_with(
        &self,
        auth: &dyn Authorizer,
        request: UpdatePropertyRequest,
    ) -> Result<Batch> {
        self.update_property(
            auth,
            &request.graph,
            &request.entity_id,
            &request.predicate,
            request.old_value.as_deref(),
            &request.new_value,
        )
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
        let batch = self.manager().apply_property_update(
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
    /// Visibility is decided lazily, once per graph the evaluation actually
    /// touches, instead of by materializing the whole visible set up front:
    /// enumerating every graph cost a term decode, a full metadata decode and
    /// an authorization call per graph in the corpus, even for a query that
    /// reads one graph (finding R1).
    ///
    /// The predicate cannot report failure, so a store read error must deny.
    /// `ensure_graph_action` already folds both "read failed" and "policy says
    /// no" into `Err`, and `is_ok()` maps that to *not visible* — never to
    /// visible (G8 soundness).
    pub fn query(&self, auth: &dyn Authorizer, sparql: &str) -> Result<QueryResults> {
        Ok(self
            .sparql
            .query_with_visibility(sparql, &|graph: &GraphId| {
                self.ensure_graph_action(graph, auth, Action::Read).is_ok()
            })?)
    }

    /// Execute a SPARQL query against an explicit set of local graphs.
    pub fn query_graphs(&self, graphs: &[GraphId], sparql: &str) -> Result<QueryResults> {
        Ok(self.sparql.query_with_graphs(sparql, graphs)?)
    }

    /// Execute a SPARQL query where graph visibility is decided by `visible`.
    ///
    /// The predicate is evaluated lazily over the union view: it runs at most
    /// once per graph the evaluation actually touches (memoized for the
    /// duration of the query), so the cost scales with the graphs a query
    /// reaches instead of the total corpus. A quad participates in evaluation
    /// iff its graph satisfies the predicate; the predicate must be cheap and
    /// side-effect free.
    pub fn query_graphs_with<F>(&self, visible: F, sparql: &str) -> Result<QueryResults>
    where
        F: Fn(&GraphId) -> bool,
    {
        Ok(self.sparql.query_with_visibility(sparql, &visible)?)
    }

    /// [`CraqleNode::query_graphs_with`] with explicit control over the
    /// craqle query-plan optimizer. `optimize = false` evaluates the raw
    /// sparopt plan; used for plan debugging and result-equivalence tests.
    /// The `CRAQLE_QUERY_OPT=off` environment variable disables the
    /// optimizer globally for the default query entry points.
    pub fn query_graphs_with_planner<F>(
        &self,
        visible: F,
        sparql: &str,
        optimize: bool,
    ) -> Result<QueryResults>
    where
        F: Fn(&GraphId) -> bool,
    {
        Ok(self
            .sparql
            .query_with_visibility_planned(sparql, &visible, optimize)?)
    }

    /// Block until the cross-graph derived indexes are built; they are kept
    /// up to date incrementally afterwards.
    pub fn ensure_query_indexes(&self) {
        self.store.ensure_derived_indexes();
    }

    /// Search visible resources in the local search index.
    ///
    /// Tantivy collects a global top-k by score with no idea of who is asking,
    /// so authorization runs afterwards against the *stored* policy — never as
    /// an index-side filter, because the index can lag the store and a policy
    /// may have changed since a document was written (G8 soundness).
    ///
    /// Filtering afterwards used to silently truncate the result: a single
    /// over-fetch of `limit * 4` returned fewer than `limit` readable hits
    /// whenever unreadable graphs dominated the top of the ranking (finding
    /// K2 — a completeness bug, not just a slow path). The loop below widens
    /// the over-fetch until either enough readable hits are found or the index
    /// is exhausted, so an authorized caller is never shown a short page while
    /// matching, readable documents exist (G8 completeness).
    pub fn search_with(
        &self,
        auth: &dyn Authorizer,
        req: SearchRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
        if req.limit == 0 {
            return Ok(Vec::new());
        }

        let mut readable = ReadableGraphs::new(self, auth);
        let mut fetch = req.limit.saturating_mul(4).max(SEARCH_MIN_FETCH);
        loop {
            let raw_hits = self.search.search(req.query, fetch)?;
            // Fewer hits than asked for means the index has nothing more to
            // give; widening again cannot produce another readable hit.
            let index_exhausted = raw_hits.len() < fetch;

            let mut hits = Vec::with_capacity(raw_hits.len().min(req.limit));
            for hit in raw_hits {
                if readable.allows(&hit.graph_id)? {
                    hits.push(hit);
                }
            }

            if hits.len() >= req.limit || index_exhausted {
                // Tantivy already returned score-descending order and the
                // filter preserves it, so no re-sort is needed here.
                hits.truncate(req.limit);
                return Ok(hits);
            }
            fetch = fetch.saturating_mul(4);
        }
    }

    #[deprecated(note = "use CraqleNode::search_with with a SearchRequest; removed in W-CLEAN")]
    pub fn search(
        &self,
        auth: &dyn Authorizer,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_with(auth, SearchRequest { query, limit })
    }

    /// Search visible resources in an explicit set of graph IRIs.
    ///
    /// Every selected graph is authorized against its stored policy *before*
    /// the index is consulted, so no post-filtering — and therefore no
    /// escalation loop — is needed: every hit the index can return already
    /// belongs to a graph the caller may read. Missing or non-readable graphs
    /// are ignored, matching [`CraqleNode::search_with`].
    pub fn search_graphs_with(
        &self,
        auth: &dyn Authorizer,
        req: GraphSearchRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
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
        // the whole set is cheaper and returns the same global top-k.
        let hits = if selected.len() <= SEARCH_GRAPHS_PER_GRAPH_LIMIT {
            let mut hits = Vec::new();
            for graph in &selected {
                hits.extend(
                    self.search
                        .search_in_graph(graph.as_str(), req.query, req.limit)?,
                );
            }
            limit_search_hits(hits, req.limit)
        } else {
            self.search.search_in_graphs(search::GraphSetQuery {
                graphs: &selected,
                query: req.query,
                limit: req.limit,
            })?
        };

        Ok(hits)
    }

    #[deprecated(
        note = "use CraqleNode::search_graphs_with with a GraphSearchRequest; removed in W-CLEAN"
    )]
    pub fn search_graphs(
        &self,
        auth: &dyn Authorizer,
        graphs: &[GraphId],
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_graphs_with(
            auth,
            GraphSearchRequest {
                graphs,
                query,
                limit,
            },
        )
    }

    /// Resolve one visible subject into `(predicate, object)` pairs.
    pub fn describe_subject_with(
        &self,
        auth: &dyn Authorizer,
        req: DescribeRequest<'_>,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.ensure_graph_action(req.graph, auth, Action::Read)?;
        let ctx = self.describe_ctx(req.graph)?;
        self.describe_in_ctx(&ctx, req.subject_id)
    }

    #[deprecated(
        note = "use CraqleNode::describe_subject_with with a DescribeRequest; removed in W-CLEAN"
    )]
    pub fn describe_subject(
        &self,
        auth: &dyn Authorizer,
        graph: &GraphId,
        subject_id: &str,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.describe_subject_with(auth, DescribeRequest { graph, subject_id })
    }

    /// Hydrate search hits with visible RDF properties.
    ///
    /// Search results usually cluster into a handful of graphs, so the policy
    /// read and the orphan-set rebuild are memoized per graph rather than
    /// repeated per hit (finding R8). Hits in a graph the caller may not read
    /// are skipped rather than failing the whole call, matching how
    /// [`CraqleNode::search_with`] drops them.
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
    pub fn search_resources_with(
        &self,
        auth: &dyn Authorizer,
        req: SearchRequest<'_>,
    ) -> Result<Vec<HydratedSearchHit>> {
        let hits = self.search_with(auth, req)?;
        self.hydrate_search_hits(auth, &hits)
    }

    #[deprecated(
        note = "use CraqleNode::search_resources_with with a SearchRequest; removed in W-CLEAN"
    )]
    pub fn search_resources(
        &self,
        auth: &dyn Authorizer,
        query: &str,
        limit: usize,
    ) -> Result<Vec<HydratedSearchHit>> {
        self.search_resources_with(auth, SearchRequest { query, limit })
    }

    /// Block until the background full-text indexer has processed queued work.
    pub fn flush_search_updates(&self) -> Result<()> {
        self.search_worker.flush()
    }

    /// Rebuild the full-text index from store state.
    ///
    /// Commits Tantivy and persists Fjall once per batch of graphs rather than
    /// once per graph: every commit replays the queued deletes against every
    /// segment, which made a per-graph commit super-linear in corpus size
    /// (finding W8).
    pub fn reindex_search(&self) -> Result<()> {
        let mut covered = Vec::with_capacity(REINDEX_COMMIT_BATCH_GRAPHS);
        for graph in self.store.graphs()? {
            self.search.reindex_from_store(&self.store, &graph)?;
            covered.push(graph);
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
    fn commit_reindexed_graphs(&self, covered: &mut Vec<GraphId>) -> Result<()> {
        if covered.is_empty() {
            return Ok(());
        }
        self.search.commit()?;
        for graph in covered.drain(..) {
            self.store.clear_fts_queue_for_graph(&graph)?;
        }
        self.persist_fjall()
    }

    /// Run manual store compaction as a post-ingest maintenance step.
    pub fn manual_compact_store(&self) -> Result<()> {
        self.store.manual_compact()?;
        Ok(())
    }

    pub fn import_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> Result<()> {
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
            let term = self.store.decode_graph_term(graph_id?)?;
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
        self.delete_graph_unchecked(graph)
    }

    pub fn delete_graph_unchecked(&self, graph: &GraphId) -> Result<()> {
        if let Some(sync) = &self.sync
            && sync.graph_topic_id(&self.store, graph)?.is_some()
            && !self.store.graph_tombstoned(graph)?
        {
            let record = sync.publish_delete(&self.store, graph)?;
            self.apply_irokle_record(&record)?;
            return self.persist_fjall();
        }
        self.store.set_graph_tombstone(graph)?;
        self.store.delete_graph(graph)?;
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
        if !durability.publishes_irokle() {
            return self.set_local_graph_policy(graph, policy);
        }

        if let Some(sync) = &self.sync {
            if self.store.contains_graph(graph)? && self.store.graph_policy(graph)? == policy {
                return Ok(());
            }
            let record = sync.publish_policy(&self.store, graph, policy.clone())?;
            self.apply_irokle_record(&record)?;
            return Ok(());
        }

        self.set_local_graph_policy(graph, policy)
    }

    fn set_local_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> Result<()> {
        self.store.set_graph_policy(graph, &policy)?;
        Ok(())
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
            self.store.enqueue_fts_reindex_by_id(&mut batch, graph_id)?;
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

    fn apply_irokle_record(
        &self,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<bool> {
        if self.store.graph_tombstoned(record.event.graph())? {
            return Ok(false);
        }
        match &record.event {
            CraqleGraphEvent::GraphDeleted { graph } => {
                self.store.set_graph_tombstone(graph)?;
                self.store.delete_graph(graph)?;
                self.schedule_search_update();
                Ok(true)
            }
            CraqleGraphEvent::Policy { graph, policy } => {
                let policy = policy.clone().normalized();
                if self.store.graph_policy(graph)? == policy {
                    return Ok(false);
                }
                self.set_local_graph_policy(graph, policy)?;
                Ok(true)
            }
            CraqleGraphEvent::QuadChanges { graph, .. } => {
                let Some(result) = self.replication.apply_irokle_record(record)? else {
                    return Ok(false);
                };
                if result.applied {
                    self.schedule_search_update_for_graph(graph)?;
                }
                Ok(result.applied)
            }
            CraqleGraphEvent::ContextUpdated {
                graph,
                context,
                license,
                license_digest,
                tag,
            } => {
                // Deterministic last-write-wins: overwrite only when the incoming
                // tag strictly dominates the stored one. This converges to the
                // same context on every peer regardless of arrival order, since
                // the `(counter, actor)` order is total and the winning tag is
                // unique per distinct context value.
                if *tag <= self.store.graph_context_tag(graph)? {
                    return Ok(false);
                }
                self.store.set_graph_context(
                    graph,
                    context.as_deref(),
                    license.as_deref(),
                    *license_digest,
                    *tag,
                )?;
                Ok(true)
            }
        }
    }

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
            (false, Some(actor)) => RoCrateManager::new(Arc::new(ReplicationEngine::new(
                self.store.clone(),
                self.sparql.clone(),
                actor,
            ))),
        }
    }

    fn manager_for_durability(&self, durability: CraqleRequestDurability) -> RoCrateManager {
        self.manager_with(durability, None)
    }
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

/// Merge hits from several searches into one score-ordered page.
///
/// Only needed where hits arrive from more than one collection; a single
/// Tantivy search already returns score-descending order. The comparator
/// borrows the key fields instead of cloning two `String`s per hit and
/// rebuilding the key for both sides of every comparison (finding R8). Callers
/// never pass duplicate `(graph, subject)` pairs, so an unstable sort is a
/// total order here and no dedup pass is required.
fn limit_search_hits(mut hits: Vec<SearchHit>, limit: usize) -> Vec<SearchHit> {
    hits.sort_unstable_by(|left, right| {
        Reverse(score_key(left.score))
            .cmp(&Reverse(score_key(right.score)))
            .then_with(|| left.graph_id.cmp(&right.graph_id))
            .then_with(|| left.subject_iri.cmp(&right.subject_iri))
    });
    hits.truncate(limit);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer_auth() -> GrantAuthorizer {
        GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)])
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

    /// A panic inside the indexer drain must not take the worker thread down.
    ///
    /// The search index is derived state, and the thread that repairs it is the
    /// same one that drains the queue. If a panic killed it, the index would
    /// stay diverged from the store until the process restarted — the lingering
    /// inconsistency the recovery rules forbid.
    #[test]
    fn search_worker_survives_a_panicking_drain() {
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
            .search_with(
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
}
