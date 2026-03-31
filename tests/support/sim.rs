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
        peer: usize,
        auth: &dyn Authorizer,
        query: &str,
        limit: usize,
        options: QueryOptions,
    ) -> Result<Vec<SearchHit>> {
        if options.local_only {
            return Ok(self.peers[peer].search(auth, query, limit)?);
        }

        let mut hits = Vec::new();
        for index in self.federated_peer_indexes(peer) {
            hits.extend(self.peers[index].search(auth, query, limit)?);
        }

        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.graph_id.cmp(&right.graph_id))
                .then_with(|| left.subject_iri.cmp(&right.subject_iri))
        });
        hits.dedup_by(|left, right| {
            left.graph_id == right.graph_id && left.subject_iri == right.subject_iri
        });
        hits.truncate(limit);
        Ok(hits)
    }

    fn do_catchup(&self) -> Result<()> {
        let peer_count = self.peers.len();
        let graphs = self.all_graphs()?;

        for graph in &graphs {
            for receiver in 0..peer_count {
                for sender in 0..peer_count {
                    if sender == receiver || !self.connected[sender][receiver] {
                        continue;
                    }

                    if self.maybe_bootstrap_with_snapshot(sender, receiver, graph)? {
                        continue;
                    }

                    let clock = self.peers[receiver].vector_clock(graph)?;
                    self.sync_policy(sender, receiver, graph)?;
                    self.peers[receiver]
                        .apply_remote_batches(self.peers[sender].catchup_batches(graph, &clock)?)?;
                }
            }
        }

        Ok(())
    }

    fn maybe_bootstrap_with_snapshot(
        &self,
        sender: usize,
        receiver: usize,
        graph: &GraphId,
    ) -> Result<bool> {
        let receiver_clock = self.peers[receiver].vector_clock(graph)?;
        if !receiver_clock.0.is_empty() {
            return Ok(false);
        }

        let (receiver_quads, _, _) = self.peers[receiver].graph_fingerprint(graph)?;
        if receiver_quads != 0 {
            return Ok(false);
        }

        let (sender_quads, _, _) = self.peers[sender].graph_fingerprint(graph)?;
        if sender_quads < SNAPSHOT_BOOTSTRAP_THRESHOLD_QUADS {
            return Ok(false);
        }

        let policy = self.peers[sender].graph_policy(graph)?;
        let snapshot = self.peers[sender].compact_graph_snapshot(graph)?;
        match self.peers[receiver].import_compact_graph_snapshot(&snapshot, policy) {
            Ok(()) => Ok(true),
            Err(CraqleError::SyncInputRejected(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn sync_policy(&self, sender: usize, receiver: usize, graph: &GraphId) -> Result<()> {
        if self.peers[sender].contains_graph(graph)? {
            let policy = self.peers[sender].graph_policy(graph)?;
            self.peers[receiver].import_graph_policy(graph, policy)?;
        }
        Ok(())
    }

    fn all_graphs(&self) -> Result<Vec<GraphId>> {
        let mut graphs = HashSet::new();
        for peer in &self.peers {
            for graph in peer.graphs()? {
                graphs.insert(graph.as_str().to_string());
            }
        }
        let mut graphs: Vec<GraphId> = graphs
            .into_iter()
            .map(|graph| GraphId::new(&graph))
            .collect();
        graphs.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(graphs)
    }

    fn check_convergence(&self) -> Result<bool> {
        let graphs = self.all_graphs()?;

        for graph in &graphs {
            let mut reference_clock: Option<VectorClock> = None;
            let mut reference_fingerprint: Option<(u64, [u8; 32], [u8; 32])> = None;

            for (idx, peer) in self.peers.iter().enumerate() {
                let has_connection = (0..self.peers.len()).any(|other| {
                    idx != other && (self.connected[idx][other] || self.connected[other][idx])
                });
                if !has_connection {
                    continue;
                }

                let clock = peer.vector_clock(graph)?;
                if let Some(reference) = &reference_clock {
                    if *reference != clock {
                        return Ok(false);
                    }
                } else {
                    reference_clock = Some(clock);
                }

                let fingerprint = peer.graph_fingerprint(graph)?;
                if let Some(reference) = &reference_fingerprint {
                    if *reference != fingerprint {
                        return Ok(false);
                    }
                } else {
                    reference_fingerprint = Some(fingerprint);
                }
            }
        }

        Ok(true)
    }

    fn federated_peer_indexes(&self, peer: usize) -> Vec<usize> {
        let mut indexes = vec![peer];
        for other in 0..self.peers.len() {
            if other != peer && (self.connected[peer][other] || self.connected[other][peer]) {
                indexes.push(other);
            }
        }
        indexes.sort_unstable();
        indexes.dedup();
        indexes
    }
}

fn merge_query_results(results: Vec<QueryResults>) -> QueryResults {
    let Some(first) = results.first() else {
        return QueryResults::Solutions(Vec::new());
    };

    match first {
        QueryResults::Solutions(_) => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            for result in results {
                if let QueryResults::Solutions(rows) = result {
                    for row in rows {
                        let mut key: Vec<(String, EncodedTerm)> =
                            row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        key.sort_by(|left, right| left.0.cmp(&right.0));
                        if seen.insert(key) {
                            merged.push(row);
                        }
                    }
                }
            }
            QueryResults::Solutions(merged)
        }
        QueryResults::Boolean(_) => {
            QueryResults::Boolean(results.into_iter().any(|result| match result {
                QueryResults::Boolean(value) => value,
                _ => false,
            }))
        }
        QueryResults::Graph(_) => {
            let mut merged = BTreeSet::new();
            for result in results {
                if let QueryResults::Graph(triples) = result {
                    merged.extend(triples);
                }
            }
            QueryResults::Graph(merged.into_iter().collect())
        }
    }
}
