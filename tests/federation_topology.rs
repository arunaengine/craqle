#![cfg(feature = "federation-routing")]

use std::time::Duration;

use craqle::federation::{
    FederationRoutingError, FederationTopology, GraphHolder, GraphHolderHealth, GraphReplicaSet,
    ReplicaSelectionOptions, route_graph,
};
use craqle::{ActorId, CraqleError, GraphId, Result};

#[derive(Clone)]
struct StaticTopology(Option<GraphReplicaSet>);

impl FederationTopology for StaticTopology {
    fn graph_holders(&self, _graph: &GraphId) -> Result<Option<GraphReplicaSet>> {
        Ok(self.0.clone())
    }
}

fn holder(id: u8, generation: u8, health: GraphHolderHealth, latency_ms: u64) -> GraphHolder {
    GraphHolder {
        node_id: ActorId::from_bytes([id; 32]),
        graph_generation: [generation; 32],
        health,
        observed_latency: Some(Duration::from_millis(latency_ms)),
    }
}

#[test]
fn normal_graph_route_selects_one_lowest_latency_current_holder() {
    let graph = GraphId::new("urn:test:federation:graph");
    let topology = StaticTopology(Some(GraphReplicaSet {
        graph: graph.clone(),
        graph_generation: [7; 32],
        topology_generation: 11,
        primary: holder(1, 7, GraphHolderHealth::Healthy, 30),
        replicas: vec![
            holder(2, 6, GraphHolderHealth::Healthy, 1),
            holder(3, 7, GraphHolderHealth::Unhealthy, 2),
            holder(4, 7, GraphHolderHealth::Healthy, 10),
        ],
    }));
    let fingerprint = [9; 32];

    let route = route_graph(
        &topology,
        &graph,
        fingerprint,
        ReplicaSelectionOptions::default(),
    )
    .unwrap();

    assert_eq!(route.first, ActorId::from_bytes([4; 32]));
    assert_eq!(route.graph_generation, [7; 32]);
    assert_eq!(route.topology_generation, 11);
    assert_eq!(route.query_fingerprint, fingerprint);
    assert!(route.hedge.is_none());
}

#[test]
fn hedge_uses_a_distinct_holder_with_the_same_route_fences() {
    let graph = GraphId::new("urn:test:federation:hedge");
    let topology = StaticTopology(Some(GraphReplicaSet {
        graph: graph.clone(),
        graph_generation: [3; 32],
        topology_generation: 5,
        primary: holder(1, 3, GraphHolderHealth::Healthy, 20),
        replicas: vec![holder(2, 3, GraphHolderHealth::Healthy, 10)],
    }));

    let route = route_graph(
        &topology,
        &graph,
        [8; 32],
        ReplicaSelectionOptions {
            hedge_after: Some(Duration::from_millis(15)),
        },
    )
    .unwrap();

    assert_eq!(route.first, ActorId::from_bytes([2; 32]));
    assert_eq!(route.hedge.unwrap().node_id, ActorId::from_bytes([1; 32]));
    assert_eq!(route.graph_generation, [3; 32]);
    assert_eq!(route.query_fingerprint, [8; 32]);
}

#[test]
fn missing_or_noncurrent_holders_fail_closed() {
    let graph = GraphId::new("urn:test:federation:missing");
    let error = route_graph(
        &StaticTopology(None),
        &graph,
        [0; 32],
        ReplicaSelectionOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::FederationRouting(FederationRoutingError::GraphNotRouted { .. })
    ));

    let stale = StaticTopology(Some(GraphReplicaSet {
        graph: graph.clone(),
        graph_generation: [2; 32],
        topology_generation: 1,
        primary: holder(1, 1, GraphHolderHealth::Healthy, 1),
        replicas: vec![holder(2, 2, GraphHolderHealth::Unknown, 1)],
    }));
    let error =
        route_graph(&stale, &graph, [0; 32], ReplicaSelectionOptions::default()).unwrap_err();
    assert!(matches!(
        error,
        CraqleError::FederationRouting(FederationRoutingError::NoHealthyCurrentHolder { .. })
    ));
}
