//! Craqle stores, validates, queries, searches, and replicates RO-Crates.
//!
//! For application integration, prefer the root `craqle` API centered around
//! [`CraqleNode`], typed request structs, and RO-Crate JSON-LD import/export.
//! The lower-level modules exposed from `src/internal/` remain available for
//! advanced use cases and tests, but they are not the primary integration
//! surface.

#[path = "internal/core.rs"]
mod core;
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

use std::cmp::Reverse;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

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
#[doc(hidden)]
pub use crate::core::{
    CompactSnapshotQuadState, Dot, GraphReplicaCompactSnapshot, GraphReplicaSnapshot, QuadOp,
    SnapshotQuadState,
};
pub use crate::replication::{MergeError, UpdateError};
pub use crate::rocrate::{AppendDataEntitiesReport, NewDataEntity, RoCrateError, RoCratePage};
pub use crate::search::SearchHit;
pub use crate::sparql::QueryResults;
pub use auth::{
    Action, AllowAllAuthorizer, AuthorizationError, Authorizer, DenyAllAuthorizer, GrantAuthorizer,
    PermissionGrant, PermissionLevel,
};

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
    #[error("unsupported update across multiple graphs")]
    MultiGraphUpdateUnsupported,
}

pub type Result<T> = std::result::Result<T, CraqleError>;

/// Input for creating a new RO-Crate graph.
#[derive(Debug, Clone)]
pub struct CreateCrateRequest {
    pub graph: GraphId,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: String,
    pub policy: GraphPolicy,
}

impl CreateCrateRequest {
    pub fn new(
        graph: GraphId,
        name: impl Into<String>,
        description: impl Into<String>,
        date_published: impl Into<String>,
        license: impl Into<String>,
        policy: GraphPolicy,
    ) -> Self {
        Self {
            graph,
            name: name.into(),
            description: description.into(),
            date_published: date_published.into(),
            license: license.into(),
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

const MAX_SYNC_BATCH_OPS: usize = 50_000;
const MAX_SYNC_POLICY_PATHS: usize = 1_024;
const MAX_SYNC_SNAPSHOT_QUADS: usize = 250_000;
const MAX_SYNC_SNAPSHOT_TERMS: usize = 500_000;
const MAX_REMOTE_BATCHES: usize = 10_000;

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
    sparql: Arc<SparqlEngine>,
    replication: Arc<ReplicationEngine>,
}

/// Configuration used when constructing a [`CraqleNode`].
pub struct CraqleOptions {
    actor: ActorId,
}

impl Default for CraqleOptions {
    fn default() -> Self {
        Self {
            actor: ActorId::random(),
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

    fn into_actor(self) -> ActorId {
        self.actor
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

        let store = Arc::new(GraphStore::open(root.join("store"))?);
        let search = Arc::new(SearchIndex::open(root.join("search"))?);
        let node = Self::from_store_and_search(store, search.clone(), options);
