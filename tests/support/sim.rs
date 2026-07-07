#![allow(dead_code)]

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use craqle::{
    Authorizer, CraqleError, CraqleGraphEvent, CraqleIrokleOptions, CraqleNode, CraqleOptions,
    EncodedTerm, GraphId, QueryResults, SearchHit, VectorClock,
};
use irokle::Event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryOptions {
    pub local_only: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("craqle: {0}")]
    Craqle(#[from] CraqleError),
    #[error("irokle: {0}")]
    Irokle(#[from] irokle::Error),
    #[error("convergence failed: {0}")]
    ConvergenceFailed(String),
}

pub type Result<T> = std::result::Result<T, SimulationError>;

pub struct CraqleCluster {
    peers: Vec<CraqleNode>,
    irokles: Vec<irokle::Irokle>,
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
        let mut irokles = Vec::with_capacity(peer_count);
        for _ in 0..peer_count {
            irokles.push(irokle::Irokle::builder().build()?);
        }
        let peer_ids: BTreeSet<_> = irokles.iter().map(irokle::Irokle::peer_id).collect();
        let mut peers = Vec::with_capacity(peer_count);
        for (idx, irokle) in irokles.iter().enumerate() {
            let initial_peers = peer_ids
                .iter()
                .copied()
                .filter(|peer| *peer != irokle.peer_id())
                .collect::<BTreeSet<_>>();
            peers.push(CraqleNode::open_with_options(
                data_dir.as_ref().join(format!("peer_{idx}")),
                options(idx).with_irokle(
                    irokle.clone(),
                    CraqleIrokleOptions::new().with_initial_peers(initial_peers),
                ),
            )?);
        }

        Ok(Self {
            peers,
            irokles,
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

    pub fn irokle(&self, index: usize) -> &irokle::Irokle {
        &self.irokles[index]
    }

    /// Sync both directions between two peers; returns the number of ops moved.
    pub fn sync_pair(&self, left: usize, right: usize) -> Result<usize> {
        if !self.connected[left][right] || !self.connected[right][left] {
            return Ok(0);
        }

        let mut moved = 0;
        for topic_id in self.all_craqle_topic_ids()? {
            moved += self.sync_topic_one_way(left, right, topic_id)?;
            moved += self.sync_topic_one_way(right, left, topic_id)?;
        }

        Ok(moved)
    }

    /// One full all-pairs sync round; returns the number of ops moved.
    pub fn sync_round(&self) -> Result<usize> {
        let peer_count = self.peers.len();
        let topics = self.all_craqle_topic_ids()?;
        let mut moved = 0;
        for sender in 0..peer_count {
            for receiver in 0..peer_count {
                if sender == receiver || !self.connected[sender][receiver] {
                    continue;
                }
                for topic_id in &topics {
                    moved += self.sync_topic_one_way(sender, receiver, *topic_id)?;
                }
            }
        }

        Ok(moved)
    }

    pub fn sync_until_converged(&self, max_rounds: usize) -> Result<()> {
        self.reconcile_all()?;

        for _ in 0..max_rounds {
            self.sync_round()?;
            self.reconcile_all()?;
            if self.check_convergence()? {
                self.flush_search_updates()?;
                return Ok(());
            }
        }

        if self.check_convergence()? {
            self.flush_search_updates()?;
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

    pub fn flush_search_updates(&self) -> Result<()> {
        for peer in &self.peers {
            peer.flush_search_updates()?;
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

    fn reconcile_all(&self) -> Result<()> {
        for peer in &self.peers {
            peer.reconcile_irokle()?;
        }
        Ok(())
    }

    fn sync_topic_one_way(
        &self,
        sender: usize,
        receiver: usize,
        topic_id: irokle::TopicId,
    ) -> Result<usize> {
        let remote_summary = self.irokles[receiver].sync_summary(topic_id)?;
        let data = self.irokles[sender]
            .plan_sync_data(self.irokles[receiver].peer_id(), &remote_summary)?;
        if data.ops.is_empty() {
            return Ok(0);
        }
        let moved = data.ops.len();
        let ack =
            self.irokles[receiver].receive_sync_data_from(self.irokles[sender].peer_id(), data)?;
        let _ = self.irokles[sender].apply_sync_ack(&ack.0);
        self.peers[receiver].reconcile_irokle()?;
        Ok(moved)
    }

    fn all_craqle_topic_ids(&self) -> Result<Vec<irokle::TopicId>> {
        let mut topics = BTreeSet::new();
        for node in &self.irokles {
            for topic in node.list_topics()? {
                if topic.event_type_id == CraqleGraphEvent::TYPE_ID {
                    topics.insert(topic.topic_id);
                }
            }
        }
        Ok(topics.into_iter().collect())
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
