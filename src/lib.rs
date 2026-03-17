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
        if search.needs_rebuild() {
            node.reindex_search()?;
        }
        Ok(node)
    }

    pub fn from_store_and_search(
        store: Arc<GraphStore>,
        search: Arc<SearchIndex>,
        options: CraqleOptions,
    ) -> Self {
        let actor = options.into_actor();
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search.clone()));
        let replication = Arc::new(ReplicationEngine::new(store.clone(), sparql.clone(), actor));

        Self {
            actor,
            store,
            search,
            sparql,
            replication,
        }
    }

    /// Return the local actor id used for authored replication batches.
    pub fn actor(&self) -> ActorId {
        self.actor
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
        let batch = self.manager().create_crate(
            graph.clone(),
            &name,
            &description,
            &date_published,
            &license,
        )?;
        self.persist_graph_policy(&graph, policy)?;
        self.finish_batch(&graph, batch)
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
        Ok(())
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
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self.manager().import_jsonld(graph.clone(), jsonld)?;
        self.persist_graph_policy(&graph, policy)?;
        self.finish_batch(&graph, batch)
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
        let policy = policy.normalized();
        self.ensure_policy_action(&graph, &policy, auth, Action::Write)?;
        let batch = self
            .manager()
            .import_jsonld_checked(graph.clone(), jsonld)?;
        self.persist_graph_policy(&graph, policy)?;
        self.finish_batch(&graph, batch)
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
