use std::sync::OnceLock;

use crate::core::{GraphId, GraphPolicy};
use globset::{Glob, GlobSet, GlobSetBuilder};

/// High-level action to authorize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
}

/// Permission level used by the built-in grant authorizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionLevel {
    Read,
    Write,
}

impl PermissionLevel {
    pub fn allows(self, action: Action) -> bool {
        match (self, action) {
            (Self::Write, _) | (Self::Read, Action::Read) => true,
            (Self::Read, Action::Write) => false,
        }
    }
}

/// One path-based permission grant used by [`GrantAuthorizer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionGrant {
    pub pattern: String,
    pub level: PermissionLevel,
}

impl PermissionGrant {
    pub fn new(pattern: impl Into<String>, level: PermissionLevel) -> Self {
        Self {
            pattern: pattern.into(),
            level,
        }
    }
}

/// Authorization failure returned by [`Authorizer`] implementations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationError {
    #[error("permission denied for {action:?} on graph `{graph}`")]
    PermissionDenied { action: Action, graph: String },
    #[error("invalid permission pattern `{pattern}`: {message}")]
    InvalidPattern { pattern: String, message: String },
}

/// Authorization hook used by the root Craqle API.
///
/// External services can implement this trait directly or pass a closure.
/// Craqle provides [`GrantAuthorizer`] as a simple built-in adapter for
/// grant/path-based authorization, but authorization policy itself is not tied
/// to that implementation.
pub trait Authorizer: Send + Sync {
    fn authorize(
        &self,
        graph: &GraphId,
        policy: &GraphPolicy,
        action: Action,
    ) -> Result<(), AuthorizationError>;
}

impl<F> Authorizer for F
where
    F: Fn(&GraphId, &GraphPolicy, Action) -> Result<(), AuthorizationError> + Send + Sync,
{
    fn authorize(
        &self,
        graph: &GraphId,
        policy: &GraphPolicy,
        action: Action,
    ) -> Result<(), AuthorizationError> {
        self(graph, policy, action)
