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
    }
}

/// Built-in authorizer adapter using path grants against graph policy paths.
/// Built-in authorizer adapter using path grants against graph policy paths.
pub struct GrantAuthorizer {
    pub grants: Vec<PermissionGrant>,
    read_matcher: OnceLock<std::result::Result<GlobSet, AuthorizationError>>,
    write_matcher: OnceLock<std::result::Result<GlobSet, AuthorizationError>>,
}

impl std::fmt::Debug for GrantAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrantAuthorizer")
            .field("grants", &self.grants)
            .finish()
    }
}

impl Clone for GrantAuthorizer {
    fn clone(&self) -> Self {
        Self::new(self.grants.clone())
    }
}

impl Default for GrantAuthorizer {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl PartialEq for GrantAuthorizer {
    fn eq(&self, other: &Self) -> bool {
        self.grants == other.grants
    }
}

impl Eq for GrantAuthorizer {}

fn compile_grant_matcher(
    grants: &[PermissionGrant],
    action: Action,
) -> std::result::Result<GlobSet, AuthorizationError> {
    let mut builder = GlobSetBuilder::new();
    for grant in grants.iter().filter(|grant| grant.level.allows(action)) {
        let glob =
            Glob::new(&grant.pattern).map_err(|error| AuthorizationError::InvalidPattern {
                pattern: grant.pattern.clone(),
                message: error.to_string(),
            })?;
        builder.add(glob);
    }

    builder
        .build()
        .map_err(|error| AuthorizationError::InvalidPattern {
            pattern: "<globset>".to_string(),
            message: error.to_string(),
        })
}

impl GrantAuthorizer {
    pub fn new(grants: Vec<PermissionGrant>) -> Self {
        Self {
            grants,
            read_matcher: OnceLock::new(),
            write_matcher: OnceLock::new(),
        }
    }

    fn matcher(&self, action: Action) -> Result<&GlobSet, AuthorizationError> {
        let cache = match action {
            Action::Read => &self.read_matcher,
            Action::Write => &self.write_matcher,
        };

        match cache.get_or_init(|| compile_grant_matcher(&self.grants, action)) {
            Ok(matcher) => Ok(matcher),
            Err(error) => Err(error.clone()),
        }
    }
}

impl Authorizer for GrantAuthorizer {
    fn authorize(
        &self,
        graph: &GraphId,
        policy: &GraphPolicy,
        action: Action,
    ) -> Result<(), AuthorizationError> {
        if action == Action::Read && policy.public {
            return Ok(());
        }

        // Test for the existence of a usable grant without materializing them:
        // `visible_graphs` and the search filters call this once per candidate
        // graph, and the collected `Vec` was allocated only to be measured.
        let has_usable_grant = self.grants.iter().any(|grant| grant.level.allows(action));

        if !has_usable_grant || policy.permission_paths.is_empty() {
            return Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_string(),
            });
        }

        let matcher = self.matcher(action)?;

        if policy
            .permission_paths
            .iter()
            .any(|path| matcher.is_match(path.as_str()))
        {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_string(),
            })
        }
    }
}

/// Authorizer that allows every request.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(
        &self,
        _graph: &GraphId,
        _policy: &GraphPolicy,
        _action: Action,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }
}

/// Authorizer that denies every request.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAllAuthorizer;

impl Authorizer for DenyAllAuthorizer {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: Action,
    ) -> Result<(), AuthorizationError> {
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_string(),
        })
    }
}
