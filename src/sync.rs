use std::collections::BTreeSet;
use std::time::Instant;

use crate::core::{
    ActorId, Batch, Dot, GraphId, GraphPolicy, MaterializedQuadChange, QuadOp, VectorClock,
};
use crate::store::GraphStore;
use chrono::Utc;
use irokle::history::HistoryOrder;
use irokle::reducer::EventRecord;
use irokle::{Event, PublishOptions, ReplicationPolicy, TopicConfig, WriteConcern};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, irokle::Event)]
#[irokle(type_id = "craqle.graph.v1")]
pub enum CraqleGraphEvent {
    QuadChanges {
        graph: GraphId,
        changes: Vec<MaterializedQuadChange>,
    },
    Policy {
        graph: GraphId,
        policy: GraphPolicy,
    },
}

impl CraqleGraphEvent {
    pub fn graph(&self) -> &GraphId {
        match self {
            Self::QuadChanges { graph, .. } | Self::Policy { graph, .. } => graph,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CraqleIrokleOptions {
    pub initial_peers: BTreeSet<irokle::PeerId>,
    pub replication_policy: ReplicationPolicy,
    /// Write concern for Craqle graph-event publishes. Defaults to local
    /// durability so Iroh async-replication bookkeeping does not block writes.
    pub write_concern: WriteConcern,
}

impl CraqleIrokleOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_initial_peers<I>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = irokle::PeerId>,
    {
        self.initial_peers = peers.into_iter().collect();
        self
    }

    pub fn with_replication_policy(mut self, policy: ReplicationPolicy) -> Self {
        self.replication_policy = policy;
        self
    }

    pub fn with_write_concern(mut self, write_concern: WriteConcern) -> Self {
        self.write_concern = write_concern;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CraqleSyncError {
    #[error("irokle: {0}")]
    Irokle(#[from] irokle::Error),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("sync backend is not configured")]
    NotConfigured,
    #[error("graph `{graph}` is already bound to irokle topic {existing}, not {incoming}")]
    TopicConflict {
        graph: String,
        existing: irokle::TopicId,
        incoming: irokle::TopicId,
    },
    #[error("invalid craqle graph event: {0}")]
    InvalidEvent(String),
}

pub type SyncResult<T> = std::result::Result<T, CraqleSyncError>;

pub(crate) trait CraqleGraphSync: Send + Sync {
    fn publish_changes(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn publish_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        policy: GraphPolicy,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn graph_topic_id(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Option<irokle::TopicId>>;

    fn ensure_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::TopicId>;

    fn bind_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        topic_id: irokle::TopicId,
    ) -> SyncResult<()>;

    fn craqle_topic_ids(&self) -> SyncResult<Vec<irokle::TopicId>>;

    fn topic_history(
        &self,
        topic_id: irokle::TopicId,
    ) -> SyncResult<Vec<EventRecord<CraqleGraphEvent>>>;

    fn add_peer(&self, store: &GraphStore, graph: &GraphId, peer: irokle::PeerId)
    -> SyncResult<()>;

    fn remove_peer(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        peer: irokle::PeerId,
    ) -> SyncResult<()>;

    fn sync_status(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Vec<irokle::SyncPeerStatus>>;
}

#[derive(Clone)]
pub struct IrokleGraphSync<S: irokle::Storage> {
    node: irokle::Irokle<S>,
    options: CraqleIrokleOptions,
}

impl<S: irokle::Storage> IrokleGraphSync<S> {
    pub fn new(node: irokle::Irokle<S>, options: CraqleIrokleOptions) -> Self {
        Self { node, options }
    }

    pub fn node(&self) -> &irokle::Irokle<S> {
        &self.node
    }

    fn open_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::Topic<CraqleGraphEvent, S>> {
        let topic_id = crate::trace_latency_step(
            "craqle.irokle.open_graph_topic",
            "ensure_graph_topic",
            graph,
            || self.ensure_graph_topic(store, graph),
        )?;
        Ok(crate::trace_latency_step(
            "craqle.irokle.open_graph_topic",
            "open_topic",
            graph,
            || self.node.open_topic::<CraqleGraphEvent>(topic_id),
        )?)
    }
}

impl<S: irokle::Storage> CraqleGraphSync for IrokleGraphSync<S> {
    fn publish_changes(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let total_started = Instant::now();
        let change_count = changes.len() as u64;
        let result = (|| {
            let topic = crate::trace_latency_step(
                "craqle.irokle.publish_changes",
                "open_graph_topic",
                graph,
                || self.open_graph_topic(store, graph),
            )?;
            Ok(crate::trace_latency_step(
                "craqle.irokle.publish_changes",
                "publish_with",
                graph,
                || {
                    topic.publish_with(
                        CraqleGraphEvent::QuadChanges {
                            graph: graph.clone(),
                            changes,
                        },
                        self.publish_options(),
                    )
                },
            )?)
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.irokle.publish_changes",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            change_count = change_count,
        );
        result
    }

    fn publish_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        policy: GraphPolicy,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let total_started = Instant::now();
        let result = (|| {
            let topic = crate::trace_latency_step(
                "craqle.irokle.publish_policy",
                "open_graph_topic",
                graph,
                || self.open_graph_topic(store, graph),
            )?;
            Ok(crate::trace_latency_step(
                "craqle.irokle.publish_policy",
                "publish_with",
                graph,
                || {
                    topic.publish_with(
                        CraqleGraphEvent::Policy {
                            graph: graph.clone(),
                            policy,
                        },
                        self.publish_options(),
                    )
                },
            )?)
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.irokle.publish_policy",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
        );
        result
    }

    fn graph_topic_id(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Option<irokle::TopicId>> {
        Ok(store
            .irokle_topic_id(graph)?
            .map(irokle::TopicId::from_bytes))
    }

    fn ensure_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::TopicId> {
        let total_started = Instant::now();
        let result = (|| {
            if let Some(topic_id) = crate::trace_latency_step(
                "craqle.irokle.ensure_graph_topic",
                "graph_topic_id",
                graph,
                || self.graph_topic_id(store, graph),
            )? {
                tracing::debug!(
                    event = "craqle.irokle.ensure_graph_topic.reuse",
                    operation = "craqle.irokle.ensure_graph_topic",
                    graph = %graph.as_str(),
                );
                return Ok(topic_id);
            }

            let topic = crate::trace_latency_step(
                "craqle.irokle.ensure_graph_topic",
                "create_topic",
                graph,
                || {
                    self.node.create_topic::<CraqleGraphEvent>(TopicConfig {
                        initial_peers: self.options.initial_peers.clone(),
                        replication_policy: self.options.replication_policy.clone(),
                    })
                },
            )?;
            let topic_id = topic.id();
            crate::trace_latency_step(
                "craqle.irokle.ensure_graph_topic",
                "store_topic_id",
                graph,
                || store.set_irokle_topic_id(graph, *topic_id.as_bytes()),
            )?;
            Ok(topic_id)
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.irokle.ensure_graph_topic",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
        );
        result
    }

    fn bind_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        topic_id: irokle::TopicId,
    ) -> SyncResult<()> {
        if let Some(existing) = self.graph_topic_id(store, graph)? {
            if existing != topic_id {
                return Err(CraqleSyncError::TopicConflict {
                    graph: graph.as_str().to_string(),
                    existing,
                    incoming: topic_id,
                });
            }
            return Ok(());
        }
        store.set_irokle_topic_id(graph, *topic_id.as_bytes())?;
        Ok(())
    }

    fn craqle_topic_ids(&self) -> SyncResult<Vec<irokle::TopicId>> {
        Ok(self
            .node
            .list_topics()?
            .into_iter()
            .filter(|topic| topic.event_type_id == CraqleGraphEvent::TYPE_ID)
            .map(|topic| topic.topic_id)
            .collect())
    }

    fn topic_history(
        &self,
        topic_id: irokle::TopicId,
    ) -> SyncResult<Vec<EventRecord<CraqleGraphEvent>>> {
        Ok(self
            .node
            .open_topic::<CraqleGraphEvent>(topic_id)?
            .history(HistoryOrder::OldestFirst)?)
    }

    fn add_peer(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        peer: irokle::PeerId,
    ) -> SyncResult<()> {
        self.open_graph_topic(store, graph)?.add_peer(peer)?;
        Ok(())
    }

    fn remove_peer(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        peer: irokle::PeerId,
    ) -> SyncResult<()> {
        self.open_graph_topic(store, graph)?.remove_peer(peer)?;
        Ok(())
    }

    fn sync_status(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Vec<irokle::SyncPeerStatus>> {
        let Some(topic_id) = self.graph_topic_id(store, graph)? else {
            return Ok(Vec::new());
        };
        Ok(self.node.sync_status(topic_id)?)
    }
}

impl<S: irokle::Storage> IrokleGraphSync<S> {
    fn publish_options(&self) -> PublishOptions {
        PublishOptions {
            write_concern: self.options.write_concern.clone(),
        }
    }
}

pub(crate) fn batch_from_irokle_record(
    record: &EventRecord<CraqleGraphEvent>,
) -> SyncResult<Option<Batch>> {
    let CraqleGraphEvent::QuadChanges { graph, changes } = &record.event else {
        return Ok(None);
    };
    let actor = actor_from_irokle(record.meta.actor_id);
    let counter = record.meta.actor_seq;
    let base_clock = clock_from_irokle(&record.meta.observed_clock);
    let dot = Dot { actor, counter };
    let mut ops = Vec::with_capacity(changes.len());

    for change in changes {
        match change {
            MaterializedQuadChange::Insert {
                graph: change_graph,
                subject,
                predicate,
                object,
            } => {
                ensure_change_graph(graph, change_graph)?;
                ops.push(QuadOp::Add {
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                    dot,
                });
            }
            MaterializedQuadChange::Delete {
                graph: change_graph,
                subject,
                predicate,
                object,
            } => {
                ensure_change_graph(graph, change_graph)?;
                ops.push(QuadOp::Remove {
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                    witnessed: base_clock.clone(),
                });
            }
        }
    }

    Ok(Some(Batch {
        graph: graph.clone(),
        actor,
        counter,
        base_clock,
        ops,
        timestamp: Utc::now(),
    }))
}

fn ensure_change_graph(expected: &GraphId, actual: &GraphId) -> SyncResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(CraqleSyncError::InvalidEvent(format!(
            "event graph `{}` contained change for `{}`",
            expected.as_str(),
            actual.as_str()
        )))
    }
}

fn actor_from_irokle(actor: irokle::ActorId) -> ActorId {
    ActorId::from_bytes(*actor.as_bytes())
}

fn clock_from_irokle(clock: &irokle::ActorClock) -> VectorClock {
    let mut out = VectorClock::new();
    for (actor, counter) in clock.iter() {
        out.advance(actor_from_irokle(*actor), *counter);
    }
    out
}
