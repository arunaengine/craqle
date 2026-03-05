#![allow(dead_code)]

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use craqle::{
    Authorizer, Batch, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId,
    GraphReplicaSnapshot, QueryResults, SearchHit, VectorClock,
};

const SNAPSHOT_BOOTSTRAP_THRESHOLD_QUADS: u64 = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryOptions {
    pub local_only: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("craqle: {0}")]
    Craqle(#[from] CraqleError),
    #[error("convergence failed: {0}")]
    ConvergenceFailed(String),
}

pub type Result<T> = std::result::Result<T, SimulationError>;

pub struct CraqleCluster {
    peers: Vec<CraqleNode>,
    connected: Vec<Vec<bool>>,
}

impl CraqleCluster {
    pub fn new(peer_count: usize, data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::new_with_options(peer_count, data_dir, |_| CraqleOptions::default())
    }

    pub fn new_with_options<F>(
        peer_count: usize,
        data_dir: impl AsRef<Path>,
        mut options: F,
    ) -> Result<Self>
    where
        F: FnMut(usize) -> CraqleOptions,
    {
        let mut peers = Vec::with_capacity(peer_count);
        for idx in 0..peer_count {
            peers.push(CraqleNode::open_with_options(
                data_dir.as_ref().join(format!("peer_{idx}")),
                options(idx),
            )?);
        }

        Ok(Self {
            peers,
            connected: vec![vec![true; peer_count]; peer_count],
        })
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peer(&self, index: usize) -> &CraqleNode {
        &self.peers[index]
    }

    pub fn peer_mut(&mut self, index: usize) -> &CraqleNode {
        &self.peers[index]
    }

    pub fn deliver_batch_to_peer(&self, index: usize, batch: Batch) -> Result<()> {
        self.peers[index].apply_remote_batch(batch)?;
        Ok(())
    }

    pub fn snapshot_graph(&self, peer: usize, graph: &GraphId) -> Result<GraphReplicaSnapshot> {
        Ok(self.peers[peer].graph_snapshot(graph)?)
    }

    pub fn load_snapshot(&self, peer: usize, snapshot: &GraphReplicaSnapshot) -> Result<()> {
        let policy = self
            .peers
            .iter()
            .find_map(|node| {
                node.contains_graph(&snapshot.graph)
                    .ok()
                    .and_then(|contains| contains.then(|| node.graph_policy(&snapshot.graph).ok()))
                    .flatten()
            })
            .unwrap_or_default();
        self.peers[peer].import_graph_snapshot(snapshot, policy)?;
        Ok(())
    }

    pub fn sync_pair(&self, left: usize, right: usize) -> Result<()> {
        if !self.connected[left][right] || !self.connected[right][left] {
            return Ok(());
        }

        let graphs = self.all_graphs()?;
        for graph in &graphs {
            if self.maybe_bootstrap_with_snapshot(left, right, graph)? {
                continue;
            }
            if self.maybe_bootstrap_with_snapshot(right, left, graph)? {
                continue;
            }

            let left_clock = self.peers[left].vector_clock(graph)?;
            let right_clock = self.peers[right].vector_clock(graph)?;

            self.sync_policy(left, right, graph)?;
            self.sync_policy(right, left, graph)?;
            self.peers[right]
                .apply_remote_batches(self.peers[left].catchup_batches(graph, &right_clock)?)?;
            self.peers[left]
                .apply_remote_batches(self.peers[right].catchup_batches(graph, &left_clock)?)?;
        }

        Ok(())
    }

    pub fn sync_round(&self) -> Result<()> {
        let peer_count = self.peers.len();
        for sender in 0..peer_count {
            for receiver in 0..peer_count {
                if sender == receiver || !self.connected[sender][receiver] {
                    continue;
                }
                for graph in self.all_graphs()? {
                    self.sync_policy(sender, receiver, &graph)?;
                    let clock = self.peers[receiver].vector_clock(&graph)?;
                    self.peers[receiver].apply_remote_batches(
                        self.peers[sender].catchup_batches(&graph, &clock)?,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn sync_until_converged(&self, max_rounds: usize) -> Result<()> {
        self.do_catchup()?;

        for _ in 0..max_rounds {
            self.sync_round()?;
            self.do_catchup()?;
            if self.check_convergence()? {
                return Ok(());
            }
        }

        if self.check_convergence()? {
            Ok(())
        } else {
            Err(SimulationError::ConvergenceFailed(
                "peers did not converge within the allotted rounds".to_string(),
            ))
        }
    }

    pub fn partition(&mut self, left: usize, right: usize) {
        self.connected[left][right] = false;
        self.connected[right][left] = false;
    }

    pub fn heal(&mut self, left: usize, right: usize) {
        self.connected[left][right] = true;
        self.connected[right][left] = true;
    }

    pub fn reindex_search(&self) -> Result<()> {
        for peer in &self.peers {
            peer.reindex_search()?;
        }
        Ok(())
    }

    pub fn query_from_peer(
        &self,
        peer: usize,
        auth: &dyn Authorizer,
        sparql: &str,
        options: QueryOptions,
    ) -> Result<QueryResults> {
        if options.local_only {
            return Ok(self.peers[peer].query(auth, sparql)?);
        }

        let mut results = Vec::new();
        for index in self.federated_peer_indexes(peer) {
            results.push(self.peers[index].query(auth, sparql)?);
        }
        Ok(merge_query_results(results))
    }

    pub fn search_from_peer(
        &self,
