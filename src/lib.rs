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
