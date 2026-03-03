mod support;

#[cfg(test)]
mod tests {
    use craqle::*;
    use proptest::prelude::*;

    use crate::support::*;

    #[test]
    fn test_single_peer_create_crate() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");
        create_test_crate(&net, 0, &graph);

        // Verify the crate was created
        assert!(net.peer(0).contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_add_add_convergence() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create crate on peer 0, sync to peer 1
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Both peers add different entities (offline)
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/file_a.csv",
            "http://schema.org/MediaObject",
            "File A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/file_b.csv",
            "http://schema.org/MediaObject",
            "File B",
            vec![],
        )
        .unwrap();

        // Sync
        net.sync_until_converged(10).unwrap();

        // Both peers should have both entities
        let f0 = net.peer(0).vector_clock(&graph).unwrap();
        let f1 = net.peer(1).vector_clock(&graph).unwrap();
        assert_eq!(f0, f1, "vector clocks must match after convergence");
    }

    #[test]
    fn test_three_peer_convergence() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate1");

        // Create on peer 0, sync to all
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Partition: isolate peer 2
        net.partition(0, 2);
        net.partition(1, 2);

        // Peer 0 and 1 make changes
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/entity_a.csv",
            "http://schema.org/MediaObject",
            "Entity A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/entity_b.csv",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();

        // Peer 2 (isolated) makes a change
        let mgr2 = manager(net.peer(2));
