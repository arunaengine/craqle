use std::path::Path;
use std::sync::Arc;

use aruna_core::*;
use aruna_rdf_store::GraphStore;
use aruna_repl::ReplicationEngine;
use aruna_search::SearchIndex;

const SNAPSHOT_BOOTSTRAP_THRESHOLD_QUADS: u64 = 50_000;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("replication: {0}")]
    Replication(#[from] aruna_repl::MergeError),
    #[error("update: {0}")]
    Update(#[from] aruna_repl::UpdateError),
    #[error("search: {0}")]
    Search(#[from] aruna_search::SearchError),
    #[error("store: {0}")]
    Store(#[from] aruna_rdf_store::StoreError),
    #[error("convergence failed: {0}")]
    ConvergenceFailed(String),
    #[error("timeout")]
    Timeout,
}

/// A simulated peer node with its own store, replication engine, and search index.
pub struct PeerNode {
    pub id: ActorId,
    pub store: Arc<GraphStore>,
    pub engine: Arc<ReplicationEngine>,
    pub search: Arc<SearchIndex>,
    outbox: Vec<Batch>,
}

impl PeerNode {
    /// Execute a SPARQL update on this peer.
    /// Returns `None` if the update produced no changes.
    pub fn update(&mut self, sparql: &str) -> Result<Option<Batch>, aruna_repl::UpdateError> {
        let batch = self.engine.local_update(sparql)?;
        if let Some(ref b) = batch {
            if !b.ops.is_empty() {
                self.outbox.push(b.clone());
            }
        }
        Ok(batch)
    }

    /// Insert raw quads on this peer.
    pub fn insert_quads(
        &mut self,
        graph: &GraphId,
        quads: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
    ) -> Result<Batch, aruna_repl::UpdateError> {
        let batch = self.engine.local_insert_quads(graph, quads)?;
        if !batch.ops.is_empty() {
            self.outbox.push(batch.clone());
        }
        Ok(batch)
    }

    /// Drain outbound batches.
    fn drain_outbox(&mut self) -> Vec<Batch> {
        std::mem::take(&mut self.outbox)
    }
}

/// Channel-based sync network simulating peer-to-peer replication.
pub struct SyncNetwork {
    peers: Vec<PeerNode>,
    /// Adjacency: peers[i] can reach peers[j] iff connected[i][j]
    connected: Vec<Vec<bool>>,
}

impl SyncNetwork {
    /// Create a network with `peer_count` peers, each with isolated storage.
    pub fn new(peer_count: usize, data_dir: impl AsRef<Path>) -> Result<Self, SyncError> {
        let mut peers = Vec::with_capacity(peer_count);

        for i in 0..peer_count {
            let actor = ActorId::random();
            let store_path = data_dir.as_ref().join(format!("peer_{i}"));
            let store = Arc::new(GraphStore::open(&store_path)?);

            let search_path = data_dir.as_ref().join(format!("search_{i}"));
            let search = Arc::new(SearchIndex::open(&search_path)?);

            let sparql = Arc::new(aruna_sparql::SparqlEngine::new(
                store.clone(),
                search.clone(),
            ));
            let guards = aruna_shacl::default_guards();
            let engine = Arc::new(ReplicationEngine::new(store.clone(), sparql, guards, actor));

            peers.push(PeerNode {
                id: actor,
                store,
                engine,
                search,
                outbox: Vec::new(),
            });
        }

        // Full mesh connectivity
        let connected = vec![vec![true; peer_count]; peer_count];

        Ok(Self { peers, connected })
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peer(&self, index: usize) -> &PeerNode {
        &self.peers[index]
    }

    pub fn peer_mut(&mut self, index: usize) -> &mut PeerNode {
        &mut self.peers[index]
    }

    /// Drain one peer's outbound batches without delivering them.
    pub fn drain_peer_outbox(&mut self, index: usize) -> Vec<Batch> {
        self.peers[index].drain_outbox()
    }

    /// Deliver a specific batch directly to a peer.
    pub fn deliver_batch_to_peer(&mut self, index: usize, batch: Batch) -> Result<(), SyncError> {
        self.peers[index].engine.apply_remote_batch(batch)?;
        Ok(())
    }

    /// Sync only a specific pair of peers.
    pub fn sync_pair(&mut self, left: usize, right: usize) -> Result<(), SyncError> {
        if !self.connected[left][right] || !self.connected[right][left] {
            return Ok(());
        }

        let left_batches = self.peers[left].drain_outbox();
        let right_batches = self.peers[right].drain_outbox();

        for batch in left_batches {
            self.peers[right].engine.apply_remote_batch(batch)?;
        }
        for batch in right_batches {
            self.peers[left].engine.apply_remote_batch(batch)?;
        }

        let graphs = self.all_graphs()?;
        for graph in &graphs {
            let left_frontier = self.peers[left].store.get_frontier(graph)?;
            let right_frontier = self.peers[right].store.get_frontier(graph)?;

            if self.maybe_bootstrap_with_snapshot(left, right, graph)? {
                continue;
            }
            if self.maybe_bootstrap_with_snapshot(right, left, graph)? {
                continue;
            }

            for batch in self.peers[left]
                .engine
                .batches_for_catchup(graph, &right_frontier)?
            {
                self.peers[right].engine.apply_remote_batch(batch)?;
            }
            for batch in self.peers[right]
                .engine
                .batches_for_catchup(graph, &left_frontier)?
            {
                self.peers[left].engine.apply_remote_batch(batch)?;
            }
        }

        Ok(())
    }

    /// Export a full graph snapshot from a peer.
    pub fn snapshot_graph(
        &self,
        peer: usize,
        graph: &GraphId,
    ) -> Result<GraphReplicaSnapshot, SyncError> {
        Ok(self.peers[peer].store.graph_snapshot(graph)?)
    }

    /// Import a graph snapshot into a peer.
    pub fn load_snapshot(
        &mut self,
        peer: usize,
        snapshot: &GraphReplicaSnapshot,
    ) -> Result<(), SyncError> {
        self.peers[peer].store.import_graph_snapshot(snapshot)?;
        Ok(())
    }

    /// Deliver all pending outbound batches to connected peers (one round).
    pub fn sync_round(&mut self) -> Result<(), SyncError> {
        let n = self.peers.len();

        // Collect all outbound batches
        let mut all_outbound: Vec<Vec<Batch>> = Vec::with_capacity(n);
        for peer in &mut self.peers {
            all_outbound.push(peer.drain_outbox());
        }

        // Deliver to connected peers
        for sender_idx in 0..n {
            for batch in &all_outbound[sender_idx] {
                for receiver_idx in 0..n {
                    if sender_idx == receiver_idx {
                        continue;
                    }
                    if !self.connected[sender_idx][receiver_idx] {
                        continue;
                    }
                    self.peers[receiver_idx]
                        .engine
                        .apply_remote_batch(batch.clone())?;
                }
            }
        }

        Ok(())
    }

    /// Run sync rounds until all peers converge or timeout.
    pub fn sync_until_converged(&mut self, max_rounds: usize) -> Result<(), SyncError> {
        // First do catch-up: each peer requests missing batches from connected peers
        self.do_catchup()?;

        for _ in 0..max_rounds {
            self.sync_round()?;

            // Also do catch-up after each round
            self.do_catchup()?;

            if self.check_convergence()? {
                return Ok(());
            }
        }

        if self.check_convergence()? {
            Ok(())
        } else {
            Err(SyncError::ConvergenceFailed(
                "peers did not converge within max rounds".into(),
            ))
        }
    }

    /// Catch-up: each peer pulls missing batches from connected peers.
    fn do_catchup(&mut self) -> Result<(), SyncError> {
        let n = self.peers.len();
        let graphs = self.all_graphs()?;

        for graph in &graphs {
            for receiver_idx in 0..n {
                for sender_idx in 0..n {
                    if sender_idx == receiver_idx {
                        continue;
                    }
                    if !self.connected[sender_idx][receiver_idx] {
                        continue;
                    }

                    let receiver_frontier = self.peers[receiver_idx].store.get_frontier(graph)?;
                    if self.maybe_bootstrap_with_snapshot(sender_idx, receiver_idx, graph)? {
                        continue;
                    }
                    let batches = self.peers[sender_idx]
                        .engine
                        .batches_for_catchup(graph, &receiver_frontier)?;

                    for batch in batches {
                        self.peers[receiver_idx].engine.apply_remote_batch(batch)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn maybe_bootstrap_with_snapshot(
        &mut self,
        sender_idx: usize,
        receiver_idx: usize,
        graph: &GraphId,
    ) -> Result<bool, SyncError> {
        let receiver_frontier = self.peers[receiver_idx].store.get_frontier(graph)?;
        if !receiver_frontier.0.is_empty() {
            return Ok(false);
        }

        let (receiver_quads, _, _) = self.peers[receiver_idx].store.graph_fingerprint(graph)?;
        if receiver_quads != 0 {
            return Ok(false);
        }

        let (sender_quads, _, _) = self.peers[sender_idx].store.graph_fingerprint(graph)?;
        if sender_quads < SNAPSHOT_BOOTSTRAP_THRESHOLD_QUADS {
            return Ok(false);
        }

        let snapshot = self.peers[sender_idx].store.graph_snapshot(graph)?;
        self.peers[receiver_idx]
            .store
            .import_graph_snapshot(&snapshot)?;
        Ok(true)
    }

    fn all_graphs(&self) -> Result<Vec<GraphId>, SyncError> {
        let mut all = std::collections::HashSet::new();
        for peer in &self.peers {
            for g in peer.store.graphs()? {
                all.insert(g.as_str().to_string());
            }
        }
        Ok(all.into_iter().map(|s| GraphId::new(&s)).collect())
    }

    /// Check if all connected peers have identical frontiers.
    fn check_convergence(&self) -> Result<bool, SyncError> {
        let graphs = self.all_graphs()?;

        for graph in &graphs {
            let mut reference_frontier: Option<Frontier> = None;
            let mut reference_fingerprint: Option<(u64, [u8; 32], [u8; 32])> = None;

            for (i, peer) in self.peers.iter().enumerate() {
                // Only check peers connected to at least one other
                let has_connection = (0..self.peers.len()).any(|j| i != j && self.connected[i][j]);
                if !has_connection {
                    continue;
                }

                let f = peer.store.get_frontier(graph)?;
                match &reference_frontier {
                    None => reference_frontier = Some(f),
                    Some(ref_f) => {
                        if *ref_f != f {
                            return Ok(false);
                        }
                    }
                }

                let fingerprint = peer.store.graph_fingerprint(graph)?;
                match &reference_fingerprint {
                    None => reference_fingerprint = Some(fingerprint),
                    Some(reference) => {
                        if *reference != fingerprint {
                            return Ok(false);
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    /// Partition: disconnect two peers.
    pub fn partition(&mut self, a: usize, b: usize) {
        self.connected[a][b] = false;
        self.connected[b][a] = false;
    }

    /// Heal: reconnect two peers.
    pub fn heal(&mut self, a: usize, b: usize) {
        self.connected[a][b] = true;
        self.connected[b][a] = true;
    }

    /// Reindex search for all peers.
    pub fn reindex_search(&self) -> Result<(), SyncError> {
        for peer in &self.peers {
            for graph in peer.store.graphs()? {
                peer.search.reindex_from_store(&peer.store, &graph)?;
            }
            peer.search.commit()?;
        }
        Ok(())
    }
}
