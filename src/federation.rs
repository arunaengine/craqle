//! Exact coordinator-facing graph topology and replica selection.
//!
//! This module routes only from authoritative graph-holder records. It does
//! not infer RDF membership or provide probabilistic membership filters.

use std::collections::HashSet;
use std::time::Duration;

use crate::{ActorId, CraqleErrorKind, GraphId, Result};

/// Stable identity of one Craqle node in an exact federation topology.
pub type NodeId = ActorId;

/// Failure to select an exact graph holder.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum FederationRoutingError {
    #[error("graph `{graph}` has no exact topology entry")]
    GraphNotRouted { graph: String },
    #[error("graph `{graph}` has no healthy holder at its current generation")]
    NoHealthyCurrentHolder { graph: String },
    #[error("topology returned holder data for `{actual}` while `{requested}` was requested")]
    GraphMismatch { requested: String, actual: String },
}

impl FederationRoutingError {
    pub fn kind(&self) -> CraqleErrorKind {
        match self {
            Self::GraphMismatch { .. } => CraqleErrorKind::InvalidInput,
            Self::GraphNotRouted { .. } | Self::NoHealthyCurrentHolder { .. } => {
                CraqleErrorKind::DependencyUnavailable
            }
        }
    }
}

/// Coordinator observation used to decide whether a holder may receive work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphHolderHealth {
    Healthy,
    Unhealthy,
    Unknown,
}

/// One primary or replica holding an exact graph generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphHolder {
    pub node_id: NodeId,
    pub graph_generation: [u8; 32],
    pub health: GraphHolderHealth,
    pub observed_latency: Option<Duration>,
}

/// Authoritative holder set for one graph at one topology generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphReplicaSet {
    pub graph: GraphId,
    pub graph_generation: [u8; 32],
    pub topology_generation: u64,
    pub primary: GraphHolder,
    pub replicas: Vec<GraphHolder>,
}

/// Exact topology supplied by a federation coordinator or control plane.
pub trait FederationTopology: Send + Sync {
    fn graph_holders(&self, graph: &GraphId) -> Result<Option<GraphReplicaSet>>;
}

/// Optional hedge timing for exact graph routing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaSelectionOptions {
    pub hedge_after: Option<Duration>,
}

/// A second holder eligible for the same graph request after a delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HedgedGraphRequest {
    pub node_id: NodeId,
    pub after: Duration,
}

/// One exact graph route. The first and hedge share its generation and query fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRoute {
    pub graph: GraphId,
    pub graph_generation: [u8; 32],
    pub topology_generation: u64,
    pub query_fingerprint: [u8; 32],
    pub first: NodeId,
    pub hedge: Option<HedgedGraphRequest>,
}

/// Select one healthy current holder, plus an optional current-generation hedge.
pub fn route_graph(
    topology: &dyn FederationTopology,
    graph: &GraphId,
    query_fingerprint: [u8; 32],
    options: ReplicaSelectionOptions,
) -> Result<GraphRoute> {
    let holders =
        topology
            .graph_holders(graph)?
            .ok_or_else(|| FederationRoutingError::GraphNotRouted {
                graph: graph.to_string(),
            })?;
    if holders.graph != *graph {
        return Err(FederationRoutingError::GraphMismatch {
            requested: graph.to_string(),
            actual: holders.graph.to_string(),
        }
        .into());
    }

    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for (holder, primary) in std::iter::once((&holders.primary, true))
        .chain(holders.replicas.iter().map(|holder| (holder, false)))
    {
        if holder.health == GraphHolderHealth::Healthy
            && holder.graph_generation == holders.graph_generation
            && seen.insert(holder.node_id)
        {
            candidates.push((
                holder.node_id,
                holder.observed_latency.unwrap_or(Duration::MAX),
                primary,
            ));
        }
    }
    candidates.sort_by_key(|(node_id, latency, primary)| (*latency, !*primary, *node_id));

    let Some((first, _, _)) = candidates.first().copied() else {
        return Err(FederationRoutingError::NoHealthyCurrentHolder {
            graph: graph.to_string(),
        }
        .into());
    };
    let hedge = options.hedge_after.and_then(|after| {
        candidates.get(1).map(|(node_id, _, _)| HedgedGraphRequest {
            node_id: *node_id,
            after,
        })
    });
    Ok(GraphRoute {
        graph: graph.clone(),
        graph_generation: holders.graph_generation,
        topology_generation: holders.topology_generation,
        query_fingerprint,
        first,
        hedge,
    })
}
