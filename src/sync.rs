use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, PoisonError, RwLock};

use crate::core::{
    ActorId, Batch, ContextTag, Dot, EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange,
    QuadOp, VectorClock,
};
use crate::store::GraphStore;
use chrono::Utc;
use irokle::history::HistoryOrder;
use irokle::oplog::Oplog;
use irokle::reducer::{EventRecord, OpMeta};
use irokle::{Event, PublishOptions, ReplicationPolicy, TopicGenesis, WriteConcern};
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
    GraphDeleted {
        graph: GraphId,
    },
    /// Last-write-wins update of a graph's raw RO-Crate render hints.
    ///
    /// `context` and `license` hold the raw submitted JSON shapes. `tag` is the
    /// last-write-wins ordering tag: a receiving peer overwrites its stored hints
    /// only when this tag strictly dominates its own, so all peers converge on
    /// the same rendering regardless of event arrival order.
    ContextUpdated {
        graph: GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
        tag: ContextTag,
    },
}

impl CraqleGraphEvent {
    pub fn graph(&self) -> &GraphId {
        match self {
            Self::QuadChanges { graph, .. }
            | Self::Policy { graph, .. }
            | Self::GraphDeleted { graph }
            | Self::ContextUpdated { graph, .. } => graph,
        }
    }
}

pub(crate) struct TopicCatchup {
    pub records: Vec<EventRecord<CraqleGraphEvent>>,
    pub cursor: TopicCursor,
}

/// How far a reconcile pass has consumed a topic's history.
///
/// Records are consumed one at a time, so a record the pass could not apply
/// leaves the cursor behind it and the next pass redelivers it (G3).
pub(crate) struct TopicCursor {
    clock: irokle::ActorClock,
    consumed: bool,
}

impl TopicCursor {
    fn resuming(clock: irokle::ActorClock) -> Self {
        Self {
            clock,
            consumed: false,
        }
    }

    pub(crate) fn consume(&mut self, record: &EventRecord<CraqleGraphEvent>) {
        self.clock
            .observe(record.meta.actor_id, record.meta.actor_seq);
        self.consumed = true;
    }

    /// `None` until a record has been consumed, so a pass that stalls on the
    /// first one leaves the stored cursor untouched.
    pub(crate) fn encode(&self) -> SyncResult<Option<Vec<u8>>> {
        if !self.consumed {
            return Ok(None);
        }
        postcard::to_allocvec(&self.clock)
            .map(Some)
            .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))
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

pub(crate) type SyncResult<T> = std::result::Result<T, CraqleSyncError>;

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

    fn publish_delete(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn publish_context(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
        tag: ContextTag,
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

    /// Bind the graph's deterministic topic id only if its genesis is already
    /// present locally (self-minted or adopted from a peer). Never mints, so a
    /// concurrent caller cannot fork a rival genesis. Returns `None` when no
    /// genesis exists yet.
    fn bind_graph_topic_if_present(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Option<irokle::TopicId>>;

    /// Mint the graph's deterministic topic genesis with an explicit member set,
    /// or bind an existing one if a concurrent admission already created it. The
    /// single-minter discipline lives in the embedder; this is the only path
    /// that creates a graph genesis.
    fn mint_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        initial_peers: BTreeSet<irokle::PeerId>,
    ) -> SyncResult<irokle::TopicId>;

    fn craqle_topic_ids(&self) -> SyncResult<Vec<irokle::TopicId>>;

    fn topic_records_since(
        &self,
        topic_id: irokle::TopicId,
        cursor: Option<&[u8]>,
    ) -> SyncResult<TopicCatchup>;

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
    /// Memo of confirmed graph → irokle topic bindings (derived-state register
    /// row 12). Bindings are write-once for a live graph, so a hit can never be
    /// wrong while the graph exists; only *confirmed* bindings are inserted and
    /// a miss is never cached, because a concurrent sync admission can create
    /// the topic between two calls.
    ///
    /// Shared across clones so every handle to one node sees one memo.
    topic_memo: Arc<RwLock<HashMap<GraphId, irokle::TopicId>>>,
}

impl<S: irokle::Storage> IrokleGraphSync<S> {
    pub fn new(node: irokle::Irokle<S>, options: CraqleIrokleOptions) -> Self {
        Self {
            node,
            options,
            topic_memo: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn node(&self) -> &irokle::Irokle<S> {
        &self.node
    }

    fn open_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::Topic<CraqleGraphEvent, S>> {
        let topic_id = self.ensure_graph_topic(store, graph)?;
        Ok(self.node.open_topic::<CraqleGraphEvent>(topic_id)?)
    }

    fn memoized_topic(&self, graph: &GraphId) -> Option<irokle::TopicId> {
        // Guards the graph → topic memo.
        let memo = self
            .topic_memo
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        memo.get(graph).copied()
    }

    /// Record a binding the store has confirmed. Never called with a guess.
    fn remember_topic(&self, graph: &GraphId, topic_id: irokle::TopicId) {
        // Guards the graph → topic memo.
        let mut memo = self
            .topic_memo
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        memo.insert(graph.clone(), topic_id);
    }

    /// Drop a memoized binding. Deleting a graph drops its metadata record, and
    /// with it the stored binding, so the memo must not outlive it.
    fn forget_topic(&self, graph: &GraphId) {
        // Guards the graph → topic memo.
        let mut memo = self
            .topic_memo
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        memo.remove(graph);
    }

    /// Persist a binding whose topic is known to exist, then memoize it.
    ///
    /// The store write comes first: the memo must never claim a binding the
    /// durable record does not have.
    fn bind_confirmed_topic(
        &self,
        store: &GraphStore,
        binding: GraphTopic<'_>,
    ) -> SyncResult<irokle::TopicId> {
        let GraphTopic { graph, topic_id } = binding;
        store.set_irokle_topic_id(graph, *topic_id.as_bytes())?;
        self.remember_topic(graph, topic_id);
        Ok(topic_id)
    }
}

/// A graph together with the irokle topic it is (to be) bound to.
struct GraphTopic<'a> {
    graph: &'a GraphId,
    topic_id: irokle::TopicId,
}

impl<S: irokle::Storage> CraqleGraphSync for IrokleGraphSync<S> {
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str(), change_count = changes.len()))]
    fn publish_changes(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        Ok(topic.publish_with(
            CraqleGraphEvent::QuadChanges {
                graph: graph.clone(),
                changes,
            },
            self.publish_options(),
        )?)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str()))]
    fn publish_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        policy: GraphPolicy,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        Ok(topic.publish_with(
            CraqleGraphEvent::Policy {
                graph: graph.clone(),
                policy,
            },
            self.publish_options(),
        )?)
    }

    fn publish_delete(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        let record = topic.publish_with(
            CraqleGraphEvent::GraphDeleted {
                graph: graph.clone(),
            },
            self.publish_options(),
        )?;
        // The delete is now durable in the topic, so the graph's metadata record
        // (which carries the stored binding) is about to go away everywhere.
        self.forget_topic(graph);
        Ok(record)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str()))]
    fn publish_context(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
        tag: ContextTag,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        Ok(topic.publish_with(
            CraqleGraphEvent::ContextUpdated {
                graph: graph.clone(),
                context,
                license,
                license_digest,
                tag,
            },
            self.publish_options(),
        )?)
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

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str()))]
    fn ensure_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::TopicId> {
        // Memo hit, confirmed against graph existence. The metadata record is
        // where the binding lives, so a graph that no longer has one must fall
        // through and re-bind; the probe is two key lookups against the decode
        // of a metadata record that may carry a multi-kilobyte `@context`.
        if let Some(topic_id) = self.memoized_topic(graph)
            && store.contains_graph(graph)?
        {
            return Ok(topic_id);
        }

        // An existing binding (e.g. from data created before deterministic
        // topic ids) stays authoritative for its graph.
        if let Some(topic_id) = self.graph_topic_id(store, graph)? {
            self.remember_topic(graph, topic_id);
            return Ok(topic_id);
        }

        let topic_id = graph_topic_id(graph);
        let mut genesis_error = None;
        for _ in 0..2 {
            if let Some(state) = self.node.storage().topic_state(&topic_id)? {
                if state.event_type_id != CraqleGraphEvent::TYPE_ID {
                    return Err(CraqleSyncError::Irokle(irokle::Error::EventTypeMismatch {
                        expected: CraqleGraphEvent::TYPE_ID.to_owned(),
                        actual: state.event_type_id,
                    }));
                }
                return self.bind_confirmed_topic(store, GraphTopic { graph, topic_id });
            }

            let actor_id = irokle::actor_id_for(topic_id, self.node.peer_id());
            let genesis = TopicGenesis {
                event_type_id: CraqleGraphEvent::TYPE_ID.to_owned(),
                initial_peers: self.options.initial_peers.clone(),
                replication_policy: self.options.replication_policy.clone(),
            };
            let oplog = Oplog::with_storage(self.node.storage().clone());
            match oplog.create_topic_genesis(topic_id, actor_id, genesis, self.node.signer()) {
                Ok(_) => {
                    return self.bind_confirmed_topic(store, GraphTopic { graph, topic_id });
                }
                // A concurrent sync admission may create the topic between the
                // state read and the genesis commit; re-check and reuse it. This
                // is exactly why a *missing* binding is never memoized.
                Err(error) => genesis_error = Some(error),
            }
        }
        Err(CraqleSyncError::Irokle(genesis_error.unwrap_or_else(
            || irokle::Error::Storage(format!("failed to ensure craqle topic {topic_id}")),
        )))
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
            self.remember_topic(graph, existing);
            return Ok(());
        }
        self.bind_confirmed_topic(store, GraphTopic { graph, topic_id })?;
        Ok(())
    }

    fn bind_graph_topic_if_present(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Option<irokle::TopicId>> {
        if let Some(topic_id) = self.graph_topic_id(store, graph)? {
            return Ok(Some(topic_id));
        }
        let topic_id = graph_topic_id(graph);
        let Some(state) = self.node.storage().topic_state(&topic_id)? else {
            return Ok(None);
        };
        if state.event_type_id != CraqleGraphEvent::TYPE_ID {
            return Err(CraqleSyncError::Irokle(irokle::Error::EventTypeMismatch {
                expected: CraqleGraphEvent::TYPE_ID.to_owned(),
                actual: state.event_type_id,
            }));
        }
        store.set_irokle_topic_id(graph, *topic_id.as_bytes())?;
        Ok(Some(topic_id))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str(), member_count = initial_peers.len()))]
    fn mint_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        initial_peers: BTreeSet<irokle::PeerId>,
    ) -> SyncResult<irokle::TopicId> {
        let topic_id = graph_topic_id(graph);
        let mut genesis_error = None;
        for _ in 0..2 {
            if let Some(topic_id) = self.bind_graph_topic_if_present(store, graph)? {
                return Ok(topic_id);
            }
            let actor_id = irokle::actor_id_for(topic_id, self.node.peer_id());
            let genesis = TopicGenesis {
                event_type_id: CraqleGraphEvent::TYPE_ID.to_owned(),
                initial_peers: initial_peers.clone(),
                replication_policy: self.options.replication_policy.clone(),
            };
            let oplog = Oplog::with_storage(self.node.storage().clone());
            match oplog.create_topic_genesis(topic_id, actor_id, genesis, self.node.signer()) {
                Ok(_) => {
                    store.set_irokle_topic_id(graph, *topic_id.as_bytes())?;
                    return Ok(topic_id);
                }
                Err(error) => genesis_error = Some(error),
            }
        }
        Err(CraqleSyncError::Irokle(genesis_error.unwrap_or_else(
            || irokle::Error::Storage(format!("failed to mint craqle topic {topic_id}")),
        )))
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

    fn topic_records_since(
        &self,
        topic_id: irokle::TopicId,
        cursor: Option<&[u8]>,
    ) -> SyncResult<TopicCatchup> {
        let clock: irokle::ActorClock = match cursor {
            Some(bytes) => postcard::from_bytes(bytes).unwrap_or_default(),
            None => irokle::ActorClock::default(),
        };
        let topic = self.node.open_topic::<CraqleGraphEvent>(topic_id)?;
        let records = topic.history_after(&clock, HistoryOrder::OldestFirst)?;
        Ok(TopicCatchup {
            records,
            cursor: TopicCursor::resuming(clock),
        })
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

/// Deterministic per-graph topic id: every node derives the same irokle topic
/// from the graph IRI alone, so no binding propagation is needed to agree.
pub(crate) fn graph_topic_id(graph: &GraphId) -> irokle::TopicId {
    let mut hasher = blake3::Hasher::new_derive_key("craqle-graph-topic-v1");
    hasher.update(graph.as_str().as_bytes());
    irokle::TopicId::from_bytes(*hasher.finalize().as_bytes())
}

/// The graph an event targets plus the irokle metadata that dates it.
struct EventBatchCtx<'a> {
    graph: &'a GraphId,
    meta: &'a OpMeta,
}

/// Turn one event's changes into a replication [`Batch`].
///
/// Op order is the event's change order, unchanged — irokle delivers records in
/// causal order and craqle applies them in delivery order (G3), so reordering
/// here would break both the OR-Set semantics of a delete-then-add pair and the
/// publish-first contract (G4).
fn batch_from_changes<I>(cx: EventBatchCtx<'_>, changes: I) -> SyncResult<Batch>
where
    I: IntoIterator<Item = MaterializedQuadChange>,
{
    let EventBatchCtx { graph, meta } = cx;
    let actor = actor_from_irokle(meta.actor_id);
    let counter = meta.actor_seq;
    let base_clock = clock_from_irokle(&meta.observed_clock);
    let dot = Dot { actor, counter };

    let changes = changes.into_iter();
    let mut ops = Vec::with_capacity(changes.size_hint().0);
    for change in changes {
        match change {
            MaterializedQuadChange::Insert {
                graph: change_graph,
                subject,
                predicate,
                object,
            } => {
                ensure_change_graph(graph, &change_graph)?;
                ops.push(QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                });
            }
            MaterializedQuadChange::Delete {
                graph: change_graph,
                subject,
                predicate,
                object,
            } => {
                ensure_change_graph(graph, &change_graph)?;
                ops.push(QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed: base_clock.clone(),
                });
            }
        }
    }

    Ok(Batch {
        graph: graph.clone(),
        actor,
        counter,
        base_clock,
        ops,
        timestamp: Utc::now(),
    })
}

/// Largest term craqle accepts from a topic. Well past any real IRI or literal,
/// and small enough that one record cannot be an allocation attack.
const MAX_TERM_BYTES: usize = 4 * 1024 * 1024;

/// Reject a term the store could only fail on: oversized, or outside the three
/// N-Triples shapes craqle encodes.
fn check_term(term: &EncodedTerm) -> SyncResult<()> {
    let text = term.0.as_str();
    if text.len() > MAX_TERM_BYTES {
        return Err(CraqleSyncError::InvalidEvent(format!(
            "term of {} bytes exceeds the {MAX_TERM_BYTES} byte limit",
            text.len()
        )));
    }
    let shaped = (text.starts_with('<') && text.ends_with('>') && text.len() > 1)
        || (text.starts_with('"') && text.len() > 1)
        || text.starts_with("_:");
    if shaped {
        Ok(())
    } else {
        Err(CraqleSyncError::InvalidEvent(format!(
            "term `{}` is not an encoded IRI, literal or blank node",
            text.chars().take(64).collect::<String>()
        )))
    }
}

/// Validate every term a record carries before any of it reaches the store, so
/// content a retry could never accept is rejected here.
fn check_changes(changes: &[MaterializedQuadChange]) -> SyncResult<()> {
    for change in changes {
        let terms = match change {
            MaterializedQuadChange::Insert {
                subject,
                predicate,
                object,
                ..
            }
            | MaterializedQuadChange::Delete {
                subject,
                predicate,
                object,
                ..
            } => [subject, predicate, object],
        };
        for term in terms {
            check_term(term)?;
        }
    }
    Ok(())
}

/// Borrowing variant, for callers that only hold a reference to the record
/// (catch-up and reconcile replay both re-read their records afterwards).
pub(crate) fn batch_from_irokle_record(
    record: &EventRecord<CraqleGraphEvent>,
) -> SyncResult<Option<Batch>> {
    let CraqleGraphEvent::QuadChanges { graph, changes } = &record.event else {
        return Ok(None);
    };
    check_changes(changes)?;
    let cx = EventBatchCtx {
        graph,
        meta: &record.meta,
    };
    batch_from_changes(cx, changes.iter().cloned()).map(Some)
}

/// Consuming variant: moves every term string out of the record instead of
/// cloning it, for callers that drop the record right after.
pub(crate) fn batch_from_irokle_event_record(
    record: EventRecord<CraqleGraphEvent>,
) -> SyncResult<Option<Batch>> {
    let EventRecord { event, meta } = record;
    let CraqleGraphEvent::QuadChanges { graph, changes } = event else {
        return Ok(None);
    };
    check_changes(&changes)?;
    let cx = EventBatchCtx {
        graph: &graph,
        meta: &meta,
    };
    batch_from_changes(cx, changes).map(Some)
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
