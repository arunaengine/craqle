use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::*;
use crate::rules::{GraphSnapshot, Rule};
use crate::sparql::SparqlEngine;
use crate::store::GraphStore;
use chrono::Utc;
use oxrdf::NamedNode;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("sparql: {0}")]
    Sparql(#[from] crate::sparql::SparqlError),
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<CrateViolation>),
    #[error("invalid change set: {0}")]
    InvalidChangeSet(String),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("input rejected: {0}")]
    InputRejected(String),
}

#[derive(Debug)]
pub struct MergeResult {
    pub applied: bool,
}

/// The replication engine: local writes, CRDT merge, catch-up.
pub struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    rules: Vec<Box<dyn Rule>>,
    actor: ActorId,
    gap_buffer: std::sync::Mutex<HashMap<GraphId, Vec<Batch>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsRefresh {
    Immediate,
    Deferred,
