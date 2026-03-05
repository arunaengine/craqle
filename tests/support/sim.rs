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

