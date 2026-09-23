//! Connects graph events, topic cursors, and Irokle replication.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock};

use crate::core::{
    ActorId, Batch, ContextTag, CrateRenderHints as RenderHints, Dot, EncodedTerm, GraphId,
    GraphTombstone, MaterializedQuadChange, QuadOp, TaggedGraphPolicy, TaggedRenderHints,
    VectorClock,
};
use crate::store::GraphStore;
use chrono::Utc;
use irokle::oplog::Oplog;
use irokle::reducer::{EventRecord, OpMeta};
use irokle::{Event, PublishOptions, ReplicationPolicy, TopicGenesis, WriteConcern};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphHints {
    pub context: Option<String>,
    pub license: Option<String>,
    pub license_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRenderHints {
    pub hints: GraphHints,
    pub tag: ContextTag,
}

impl From<TaggedRenderHints> for GraphRenderHints {
    fn from(tagged: TaggedRenderHints) -> Self {
        Self {
            hints: GraphHints {
                context: tagged.hints.context,
                license: tagged.hints.license,
                license_digest: tagged.hints.license_digest,
            },
            tag: tagged.tag,
        }
    }
}

impl From<GraphRenderHints> for TaggedRenderHints {
    fn from(wire: GraphRenderHints) -> Self {
        Self {
            hints: RenderHints {
                context: wire.hints.context,
                license: wire.hints.license,
                license_digest: wire.hints.license_digest,
            },
            tag: wire.tag,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, irokle::Event)]
#[irokle(type_id = "craqle.graph.v1")]
pub enum CraqleGraphEvent {
    QuadChanges {
        graph: GraphId,
        changes: Vec<MaterializedQuadChange>,
    },
    RoCrateMutation {
        graph: GraphId,
        changes: Vec<MaterializedQuadChange>,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
        tag: ContextTag,
    },
    Policy {
        graph: GraphId,
        tagged: TaggedGraphPolicy,
    },
    GraphDeleted {
        tombstone: GraphTombstone,
    },
    Mutation {
        id: MutationId,
        graph: GraphId,
        changes: Vec<MaterializedQuadChange>,
        render_hints: Option<GraphRenderHints>,
    },
}

impl CraqleGraphEvent {
    pub fn graph(&self) -> &GraphId {
        match self {
            Self::QuadChanges { graph, .. }
            | Self::RoCrateMutation { graph, .. }
            | Self::Policy { graph, .. }
            | Self::Mutation { graph, .. } => graph,
            Self::GraphDeleted { tombstone } => &tombstone.graph,
        }
    }
}

/// Authorization hook for graph-policy events authored by another replica.
pub trait RemotePolicyAuthorizer: Send + Sync {
    fn may_apply_policy(
        &self,
        graph: &GraphId,
        actor: &ActorId,
        policy: &crate::GraphPolicy,
    ) -> bool;
}

impl<F> RemotePolicyAuthorizer for F
where
    F: Fn(&GraphId, &ActorId, &crate::GraphPolicy) -> bool + Send + Sync,
{
    fn may_apply_policy(
        &self,
        graph: &GraphId,
        actor: &ActorId,
        policy: &crate::GraphPolicy,
    ) -> bool {
        self(graph, actor, policy)
    }
}

#[derive(Debug, Default)]
pub struct DenyRemotePolicyChanges;

impl RemotePolicyAuthorizer for DenyRemotePolicyChanges {
    fn may_apply_policy(
        &self,
        _graph: &GraphId,
        _actor: &ActorId,
        _policy: &crate::GraphPolicy,
    ) -> bool {
        false
    }
}

/// Durable metadata for one replication record Craqle refused to apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedReplicationRecord {
    pub topic: irokle::TopicId,
    pub record_id: irokle::OpId,
    pub actor: irokle::ActorId,
    pub sequence: u64,
    pub graph: Option<GraphId>,
    pub payload_digest: [u8; 32],
    pub error_kind: crate::CraqleErrorKind,
    pub reason: String,
    pub seen_count: u64,
    pub acknowledged: bool,
}

/// Audit record written by an explicit compare-and-replace cursor repair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCursorRepairAudit {
    pub topic: irokle::TopicId,
    pub old_cursor_digest: [u8; 32],
    pub replacement_cursor_digest: [u8; 32],
    pub repaired_at_unix_nanos: i64,
}

/// Stable identity used to inspect or retry one logical mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MutationId(pub [u8; 32]);

impl MutationId {
    pub fn new() -> Self {
        Self(*ActorId::random().as_bytes())
    }

    pub fn from_op(id: irokle::OpId) -> Self {
        Self(*id.as_bytes())
    }
}

impl Default for MutationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceOutcome {
    Prepared,
    Applied,
    Duplicate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistenceOutcome {
    Pending,
    Buffered,
    DataSynced,
    FullySynced,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairOutcome {
    NotRequired,
    Pending,
    Complete,
    Failed(crate::CraqleErrorKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairState {
    pub diagnostics: RepairOutcome,
    pub shacl: RepairOutcome,
    pub search: RepairOutcome,
    pub query_view: RepairOutcome,
}

/// Durable state of a mutation after its source commit point.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub id: MutationId,
    pub admission_sequence: u64,
    pub graph: GraphId,
    pub request_digest: [u8; 32],
    pub event_id: Option<[u8; 32]>,
    pub topic: Option<irokle::TopicId>,
    pub publish_after: Option<irokle::ActorClock>,
    pub topic_epoch: Option<u64>,
    pub topic_genesis: Option<irokle::OpId>,
    pub search_token: Option<u64>,
    pub repair_graphs: Vec<GraphId>,
    pub source: SourceOutcome,
    pub persistence: PersistenceOutcome,
    pub repairs: RepairState,
    pub source_version: [u8; 32],
    pub updated_unix_nanos: i64,
}

impl MutationReceipt {
    pub(crate) fn outbound(mut self) -> Self {
        let includes_graph = self.repair_graphs.iter().any(|graph| graph == &self.graph);
        self.repair_graphs.clear();
        if includes_graph {
            self.repair_graphs.push(self.graph.clone());
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationRequest {
    pub id: MutationId,
    pub admission_sequence: Option<u64>,
    pub graph: GraphId,
    pub changes: Vec<MaterializedQuadChange>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationLookup {
    pub graph: GraphId,
    pub id: MutationId,
    pub admission_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationStatus {
    Known(Box<MutationReceipt>),
    Expired,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupProof {
    pub location: String,
    pub archive_digest: [u8; 32],
    pub source_revision: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairMode {
    DryRun,
    Apply,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairAuthority {
    History {
        topic: irokle::TopicId,
        target: irokle::ActorClock,
    },
    HealthySnapshot {
        source: String,
        digest: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairResult {
    Exact,
    Differs,
    Applied,
    Tombstoned,
    HistoryMissing,
    BackupRequired,
    ChangedDuringRepair,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairAudit {
    pub id: MutationId,
    pub graph: GraphId,
    pub mode: RepairMode,
    pub authority: RepairAuthority,
    pub before_digest: [u8; 32],
    pub after_digest: Option<[u8; 32]>,
    pub backup: Option<BackupProof>,
    pub result: RepairResult,
    pub updated_unix_nanos: i64,
}

/// Exact states compared by an authorized reconciliation dry run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairDiff {
    pub local: crate::GraphReplicaSnapshot,
    pub authoritative: crate::GraphReplicaSnapshot,
    pub unresolved: Vec<Dot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairReport {
    pub audit: RepairAudit,
    pub diff: Option<RepairDiff>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairRequest {
    pub id: MutationId,
    pub authority: RepairAuthority,
    pub authoritative: crate::GraphReplicaSnapshot,
    pub mode: RepairMode,
    pub backup: Option<BackupProof>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileSource {
    History {
        topic: irokle::TopicId,
    },
    HealthySnapshot {
        source: String,
        snapshot: crate::GraphReplicaSnapshot,
        digest: [u8; 32],
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileRequest {
    pub id: MutationId,
    pub graph: GraphId,
    pub mode: RepairMode,
    pub source: ReconcileSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRequest {
    pub topic: irokle::TopicId,
    pub graph: GraphId,
    pub target: irokle::ActorClock,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistorySnapshot {
    Live(crate::GraphReplicaSnapshot),
    Tombstoned,
}

pub(crate) struct HistoryFailure {
    pub id: MutationId,
    pub graph: GraphId,
    pub authority: RepairAuthority,
    pub mode: RepairMode,
}

pub(crate) struct TopicCatchup {
    pub records: Vec<TopicRecord>,
    pub cursor: TopicCursor,
    pub more: bool,
}

pub(crate) struct ReplicatedGraphMutation {
    pub(crate) batch: Batch,
    pub(crate) render_hints: Option<TaggedRenderHints>,
    pub(crate) mutation_id: MutationId,
    pub(crate) request_digest: [u8; 32],
    pub(crate) event_id: irokle::OpId,
}

fn request_digest(
    graph: &GraphId,
    changes: &[MaterializedQuadChange],
    hints: Option<&TaggedRenderHints>,
) -> SyncResult<[u8; 32]> {
    let bytes = postcard::to_allocvec(&(graph, changes, hints))
        .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub(crate) struct OutgoingMutation {
    pub id: MutationId,
    pub graph: GraphId,
    pub changes: Vec<MaterializedQuadChange>,
    pub render_hints: Option<TaggedRenderHints>,
}

pub(crate) struct TopicFrontier {
    pub clock: irokle::ActorClock,
    pub epoch: u64,
    pub genesis: irokle::OpId,
}

pub(crate) enum TopicRecord {
    Event(EventRecord<CraqleGraphEvent>),
    Rejected(RejectedTopicRecord),
    Control(OpMeta),
}

pub(crate) struct RejectedTopicRecord {
    pub meta: OpMeta,
    pub payload_digest: [u8; 32],
    pub error_kind: crate::CraqleErrorKind,
    pub reason: String,
}

impl TopicRecord {
    pub(crate) fn meta(&self) -> &OpMeta {
        match self {
            Self::Event(record) => &record.meta,
            Self::Rejected(record) => &record.meta,
            Self::Control(meta) => meta,
        }
    }
}

const TOPIC_PAGE_RECORDS: usize = 1024;
const TOPIC_PAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecordCandidate {
    generation: u64,
    actor: irokle::ActorId,
    sequence: u64,
    id: irokle::OpId,
}

impl Ord for RecordCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.generation, self.actor, self.sequence, self.id).cmp(&(
            other.generation,
            other.actor,
            other.sequence,
            other.id,
        ))
    }
}

impl PartialOrd for RecordCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct ActorPoint {
    actor: irokle::ActorId,
    after: u64,
}

fn next_record(
    read: &dyn irokle::storage::SnapshotRead,
    topic: &irokle::TopicId,
    point: ActorPoint,
) -> irokle::Result<Option<RecordCandidate>> {
    let Some((sequence, id)) = read
        .actor_range(topic, &point.actor, point.after, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let header = read
        .get_header(&id)?
        .ok_or_else(|| irokle::Error::Storage(format!("missing op header for {id}")))?;
    if header.topic_id != *topic || header.actor_id != point.actor || header.actor_seq != sequence {
        return Err(irokle::Error::Storage(format!(
            "actor index disagrees with op header for {id}"
        )));
    }
    Ok(Some(RecordCandidate {
        generation: header.generation,
        actor: point.actor,
        sequence,
        id,
    }))
}

/// Consumed topic history; a failed record leaves the cursor behind it for redelivery.
pub(crate) struct TopicCursor {
    state: TopicCursorPayload,
    consumed: bool,
}

impl TopicCursor {
    fn resuming(state: TopicCursorPayload) -> Self {
        Self {
            state,
            consumed: false,
        }
    }

    pub(crate) fn consume(&mut self, record: &TopicRecord) {
        let meta = record.meta();
        self.state.clock.observe(meta.actor_id, meta.actor_seq);
        self.consumed = true;
    }

    /// `None` until a record has been consumed, so a pass that stalls on the
    /// first one leaves the stored cursor untouched.
    pub(crate) fn encode(&self) -> SyncResult<Option<Vec<u8>>> {
        if !self.consumed {
            return Ok(None);
        }
        encode_topic_cursor(&self.state).map(Some)
    }
}

const TOPIC_CURSOR_VERSION: u8 = 2;

#[derive(Clone, Serialize, Deserialize)]
struct TopicCursorPayload {
    version: u8,
    topic: irokle::TopicId,
    epoch: u64,
    genesis: irokle::OpId,
    clock: irokle::ActorClock,
    target: Option<irokle::ActorClock>,
}

#[derive(Serialize, Deserialize)]
struct TopicCursorEnvelope {
    payload: TopicCursorPayload,
    checksum: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct LegacyCursorPayload {
    version: u8,
    topic: irokle::TopicId,
    clock: irokle::ActorClock,
}

#[derive(Serialize, Deserialize)]
struct LegacyCursorEnvelope {
    payload: LegacyCursorPayload,
    checksum: [u8; 32],
}

fn encode_topic_cursor(payload: &TopicCursorPayload) -> SyncResult<Vec<u8>> {
    let payload_bytes = postcard::to_allocvec(&payload)
        .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))?;
    postcard::to_allocvec(&TopicCursorEnvelope {
        payload: payload.clone(),
        checksum: *blake3::hash(&payload_bytes).as_bytes(),
    })
    .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))
}

fn decode_topic_cursor(
    expected_topic: irokle::TopicId,
    bytes: &[u8],
) -> SyncResult<TopicCursorPayload> {
    let envelope: TopicCursorEnvelope = match postcard::from_bytes(bytes) {
        Ok(envelope) => envelope,
        Err(error) => {
            if let Ok(legacy) = postcard::from_bytes::<LegacyCursorEnvelope>(bytes) {
                let payload = postcard::to_allocvec(&legacy.payload).map_err(|legacy_error| {
                    CraqleSyncError::CorruptCursor {
                        topic: expected_topic,
                        reason: legacy_error.to_string(),
                    }
                })?;
                if legacy.payload.version == 1
                    && legacy.payload.topic == expected_topic
                    && legacy.checksum == *blake3::hash(&payload).as_bytes()
                {
                    return Err(CraqleSyncError::ExpiredCursor {
                        topic: expected_topic,
                        reason:
                            "version 1 cursor lacks a branch fence; authorized repair is required"
                                .to_owned(),
                    });
                }
            }
            return Err(CraqleSyncError::CorruptCursor {
                topic: expected_topic,
                reason: error.to_string(),
            });
        }
    };
    if envelope.payload.version != TOPIC_CURSOR_VERSION {
        return Err(CraqleSyncError::CorruptCursor {
            topic: expected_topic,
            reason: format!("unsupported cursor version {}", envelope.payload.version),
        });
    }
    if envelope.payload.topic != expected_topic {
        return Err(CraqleSyncError::CorruptCursor {
            topic: expected_topic,
            reason: format!("cursor belongs to topic {}", envelope.payload.topic),
        });
    }
    let payload_bytes = postcard::to_allocvec(&envelope.payload).map_err(|error| {
        CraqleSyncError::CorruptCursor {
            topic: expected_topic,
            reason: error.to_string(),
        }
    })?;
    if envelope.checksum != *blake3::hash(&payload_bytes).as_bytes() {
        return Err(CraqleSyncError::CorruptCursor {
            topic: expected_topic,
            reason: "cursor checksum mismatch".to_owned(),
        });
    }
    Ok(envelope.payload)
}

pub fn topic_cursor_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
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
    #[error("corrupt authoritative cursor for topic {topic}: {reason}")]
    CorruptCursor {
        topic: irokle::TopicId,
        reason: String,
    },
    #[error("expired authoritative cursor for topic {topic}: {reason}")]
    ExpiredCursor {
        topic: irokle::TopicId,
        reason: String,
    },
}

impl CraqleSyncError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Store(error) => error.kind(),
            Self::NotConfigured => crate::CraqleErrorKind::DependencyUnavailable,
            Self::TopicConflict { .. } => crate::CraqleErrorKind::Conflict,
            Self::InvalidEvent(_) | Self::CorruptCursor { .. } => {
                crate::CraqleErrorKind::CorruptAuthoritativeData
            }
            Self::ExpiredCursor { .. } => crate::CraqleErrorKind::Conflict,
            Self::Irokle(_) => crate::CraqleErrorKind::Storage,
        }
    }

    /// Whether the bytes read are what failed, rather than the transport or
    /// storage carrying them. No retry can clear these.
    pub fn rejects_record(&self) -> bool {
        match self {
            Self::InvalidEvent(_) => true,
            Self::CorruptCursor { .. } | Self::ExpiredCursor { .. } => false,
            Self::Store(error) => error.rejects_record(),
            Self::Irokle(error) => matches!(
                error,
                irokle::Error::Encode(_)
                    | irokle::Error::Decode(_)
                    | irokle::Error::EventTypeMismatch { .. }
            ),
            _ => false,
        }
    }
}

pub(crate) type SyncResult<T> = std::result::Result<T, CraqleSyncError>;

pub(crate) trait CraqleGraphSync: Send + Sync {
    fn publish_mutation(
        &self,
        store: &GraphStore,
        mutation: OutgoingMutation,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        match mutation.render_hints {
            Some(hints) => {
                self.publish_rocrate_mutation(store, &mutation.graph, mutation.changes, hints)
            }
            None => self.publish_changes(store, &mutation.graph, mutation.changes),
        }
    }

    fn publish_changes(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn publish_rocrate_mutation(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        render_hints: TaggedRenderHints,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn publish_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        tagged: TaggedGraphPolicy,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>>;

    fn publish_delete(
        &self,
        store: &GraphStore,
        tombstone: GraphTombstone,
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

    /// Caller holds the graph commit guard.
    fn ensure_topic_guarded(
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

    /// Bind an existing deterministic topic genesis without minting a competing branch.
    fn bind_existing_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<Option<irokle::TopicId>>;

    /// Mint a deterministic topic with explicit members, or bind a concurrently admitted genesis.
    fn mint_graph_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        initial_peers: BTreeSet<irokle::PeerId>,
    ) -> SyncResult<irokle::TopicId>;

    fn craqle_topic_ids(&self) -> SyncResult<Vec<irokle::TopicId>>;

    /// One signed operation of `topic`, or `None` when it is missing or foreign.
    fn history_entry(
        &self,
        _topic: irokle::TopicId,
        _id: irokle::OpId,
    ) -> SyncResult<Option<crate::history::HistoryEntry>> {
        Err(CraqleSyncError::NotConfigured)
    }

    /// The causal generation of one operation of `topic`, without reading its payload.
    fn history_generation(
        &self,
        _topic: irokle::TopicId,
        _id: irokle::OpId,
    ) -> SyncResult<Option<u64>> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn topic_heads(&self, _topic: irokle::TopicId) -> SyncResult<BTreeSet<irokle::OpId>> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn topic_records_since(
        &self,
        topic_id: irokle::TopicId,
        cursor: Option<&[u8]>,
    ) -> SyncResult<TopicCatchup>;

    fn topic_cursor_at(
        &self,
        _topic_id: irokle::TopicId,
        _clock: &irokle::ActorClock,
    ) -> SyncResult<Vec<u8>> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn topic_frontier(&self, _topic_id: irokle::TopicId) -> SyncResult<TopicFrontier> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn topic_record(
        &self,
        _topic_id: irokle::TopicId,
        _id: irokle::OpId,
    ) -> SyncResult<Option<TopicRecord>> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn history_snapshot(&self, _request: &HistoryRequest) -> SyncResult<HistorySnapshot> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn find_mutation(
        &self,
        _receipt: &MutationReceipt,
    ) -> SyncResult<Option<EventRecord<CraqleGraphEvent>>> {
        Err(CraqleSyncError::NotConfigured)
    }

    fn is_local_record(
        &self,
        topic_id: irokle::TopicId,
        record: &EventRecord<CraqleGraphEvent>,
    ) -> bool;

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
    /// Shared confirmed graph-to-topic bindings. Misses remain uncached because
    /// concurrent admission may create the topic between calls.
    topic_memo: Arc<RwLock<HashMap<GraphId, irokle::TopicId>>>,
    /// Set by a test to fail the next history read, standing in for an
    /// unreadable topic. Shared across clones, like the memo.
    #[cfg(test)]
    armed_history_failure: Arc<std::sync::atomic::AtomicBool>,
}

impl<S: irokle::Storage> IrokleGraphSync<S> {
    pub fn new(node: irokle::Irokle<S>, options: CraqleIrokleOptions) -> Self {
        Self {
            node,
            options,
            topic_memo: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(test)]
            armed_history_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn node(&self) -> &irokle::Irokle<S> {
        &self.node
    }

    /// Make the next history read fail. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_history_failure(&self) {
        self.armed_history_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Consumes a pending injected failure, reporting whether one was armed.
    #[cfg(test)]
    pub(crate) fn take_history_failure(&self) -> bool {
        self.armed_history_failure
            .swap(false, std::sync::atomic::Ordering::SeqCst)
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

    fn bind_topic(
        &self,
        store: &GraphStore,
        binding: GraphTopic<'_>,
        guarded: bool,
    ) -> SyncResult<irokle::TopicId> {
        let GraphTopic { graph, topic_id } = binding;
        if guarded {
            store.set_topic_guarded(graph, *topic_id.as_bytes())?;
        } else {
            store.set_topic_id(graph, *topic_id.as_bytes())?;
        }
        self.remember_topic(graph, topic_id);
        Ok(topic_id)
    }

    fn ensure_topic(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        guarded: bool,
    ) -> SyncResult<irokle::TopicId> {
        if let Some(topic_id) = self.memoized_topic(graph)
            && store.contains_graph(graph)?
        {
            return Ok(topic_id);
        }
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
                return self.bind_topic(store, GraphTopic { graph, topic_id }, guarded);
            }

            let actor_id = irokle::actor_id_for(topic_id, self.node.peer_id());
            let genesis = TopicGenesis {
                event_type_id: CraqleGraphEvent::TYPE_ID.to_owned(),
                initial_peers: self.options.initial_peers.clone(),
                replication_policy: self.options.replication_policy.clone(),
            };
            let oplog = Oplog::with_storage(self.node.storage().clone());
            match oplog.create_topic_genesis(topic_id, actor_id, genesis, self.node.signer()) {
                Ok(_) => return self.bind_topic(store, GraphTopic { graph, topic_id }, guarded),
                Err(error) => genesis_error = Some(error),
            }
        }
        Err(CraqleSyncError::Irokle(genesis_error.unwrap_or_else(
            || irokle::Error::Storage(format!("failed to ensure craqle topic {topic_id}")),
        )))
    }
}

/// A graph together with the irokle topic it is (to be) bound to.
struct GraphTopic<'a> {
    graph: &'a GraphId,
    topic_id: irokle::TopicId,
}

impl<S: irokle::Storage> CraqleGraphSync for IrokleGraphSync<S> {
    fn publish_mutation(
        &self,
        store: &GraphStore,
        mutation: OutgoingMutation,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, &mutation.graph)?;
        Ok(topic.publish_with(
            CraqleGraphEvent::Mutation {
                id: mutation.id,
                graph: mutation.graph,
                changes: mutation.changes,
                render_hints: mutation.render_hints.map(Into::into),
            },
            self.publish_options(),
        )?)
    }

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

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str(), change_count = changes.len()))]
    fn publish_rocrate_mutation(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        render_hints: TaggedRenderHints,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        Ok(topic.publish_with(
            CraqleGraphEvent::RoCrateMutation {
                graph: graph.clone(),
                changes,
                context: render_hints.hints.context,
                license: render_hints.hints.license,
                license_digest: render_hints.hints.license_digest,
                tag: render_hints.tag,
            },
            self.publish_options(),
        )?)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str()))]
    fn publish_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        mut tagged: TaggedGraphPolicy,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let topic = self.open_graph_topic(store, graph)?;
        tagged.tag.actor = actor_from_irokle(irokle::actor_id_for(topic.id(), self.node.peer_id()));
        Ok(topic.publish_with(
            CraqleGraphEvent::Policy {
                graph: graph.clone(),
                tagged,
            },
            self.publish_options(),
        )?)
    }

    fn publish_delete(
        &self,
        store: &GraphStore,
        tombstone: GraphTombstone,
    ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
        let graph = tombstone.graph.clone();
        let topic = self.open_graph_topic(store, &graph)?;
        let record = topic.publish_with(
            CraqleGraphEvent::GraphDeleted { tombstone },
            self.publish_options(),
        )?;
        // The delete is now durable in the topic, so the graph's metadata record
        // (which carries the stored binding) is about to go away everywhere.
        self.forget_topic(&graph);
        Ok(record)
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
        self.ensure_topic(store, graph, false)
    }

    fn ensure_topic_guarded(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> SyncResult<irokle::TopicId> {
        self.ensure_topic(store, graph, true)
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
        self.bind_topic(store, GraphTopic { graph, topic_id }, false)?;
        Ok(())
    }

    fn bind_existing_topic(
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
        store.set_topic_id(graph, *topic_id.as_bytes())?;
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
            if let Some(topic_id) = self.bind_existing_topic(store, graph)? {
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
                    store.set_topic_id(graph, *topic_id.as_bytes())?;
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

    fn history_entry(
        &self,
        topic: irokle::TopicId,
        id: irokle::OpId,
    ) -> SyncResult<Option<crate::history::HistoryEntry>> {
        let Some(op) = self.node.storage().get_op(&id)? else {
            return Ok(None);
        };
        op.validate()?;
        if op.id != id {
            return Err(CraqleSyncError::InvalidEvent(
                "stored history operation has a different id".into(),
            ));
        }
        if op.signed.body.topic_id != topic {
            return Ok(None);
        }
        let irokle::TopicPayload::Event(envelope) = &op.signed.body.payload else {
            return Ok(Some(crate::history::HistoryEntry {
                op,
                record: None,
                rejected: false,
            }));
        };
        let stored = self
            .node
            .storage()
            .get_meta(&op.id)?
            .ok_or_else(|| irokle::Error::Storage(format!("missing op meta for {}", op.id)))?;
        // Like reconcile, an undecodable payload is a rejected record, not a failed read.
        let record = envelope
            .decode_event::<CraqleGraphEvent>()
            .ok()
            .map(|event| EventRecord {
                event,
                meta: OpMeta {
                    op_id: op.id,
                    actor_id: stored.actor_id,
                    actor_seq: stored.actor_seq,
                    observed_clock: stored.observed_clock,
                },
            });
        let rejected = record.is_none();
        Ok(Some(crate::history::HistoryEntry {
            op,
            record,
            rejected,
        }))
    }

    fn history_generation(
        &self,
        topic: irokle::TopicId,
        id: irokle::OpId,
    ) -> SyncResult<Option<u64>> {
        Ok(self
            .node
            .storage()
            .get_position(&id)?
            .filter(|position| position.topic_id == topic)
            .map(|position| position.generation))
    }

    fn topic_heads(&self, topic: irokle::TopicId) -> SyncResult<BTreeSet<irokle::OpId>> {
        Ok(self.node.storage().heads(&topic)?)
    }

    fn topic_records_since(
        &self,
        topic_id: irokle::TopicId,
        cursor: Option<&[u8]>,
    ) -> SyncResult<TopicCatchup> {
        #[cfg(test)]
        if self.take_history_failure() {
            return Err(CraqleSyncError::Irokle(irokle::Error::Storage(
                "injected history failure".to_owned(),
            )));
        }
        let stored = cursor
            .map(|bytes| decode_topic_cursor(topic_id, bytes))
            .transpose()?;
        let page = self.node.storage().read_snapshot(|read| {
            let view = read.topic_view(&topic_id, None)?.ok_or_else(|| {
                irokle::Error::Storage(format!("missing topic state for {topic_id}"))
            })?;
            let mut state = stored.unwrap_or_else(|| TopicCursorPayload {
                version: TOPIC_CURSOR_VERSION,
                topic: topic_id,
                epoch: view.epoch,
                genesis: view.state.genesis,
                clock: irokle::ActorClock::default(),
                target: None,
            });
            if state.epoch != view.epoch || state.genesis != view.state.genesis {
                return Err(irokle::Error::Storage(
                    "expired replication cursor: topic branch changed".to_owned(),
                ));
            }
            let target = state.target.clone().unwrap_or_else(|| view.clock.clone());
            for (actor, sequence) in state.clock.iter() {
                if *sequence > target.get(actor) {
                    return Err(irokle::Error::Storage(format!(
                        "expired replication cursor: progress exceeds target for actor {actor}"
                    )));
                }
            }
            let mut candidates = Vec::with_capacity(target.iter().count());
            for (actor, target_sequence) in target.iter() {
                let after = state.clock.get(actor);
                if after >= *target_sequence {
                    continue;
                }
                if let Some(candidate) = next_record(
                    read,
                    &topic_id,
                    ActorPoint {
                        actor: *actor,
                        after,
                    },
                )? {
                    if candidate.sequence > *target_sequence {
                        return Err(irokle::Error::Storage(format!(
                            "expired replication cursor: position missing for actor {actor}"
                        )));
                    }
                    candidates.push(candidate);
                } else {
                    return Err(irokle::Error::Storage(format!(
                        "expired replication cursor: position missing for actor {actor}"
                    )));
                }
            }

            let mut records = Vec::new();
            let mut used_bytes = 0usize;
            while records.len() < TOPIC_PAGE_RECORDS && !candidates.is_empty() {
                let index = candidates
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, candidate)| **candidate)
                    .map(|(index, _)| index)
                    .expect("non-empty candidate set has a minimum");
                let candidate = candidates.swap_remove(index);
                let mut op_bytes = 0usize;
                let mut over_budget = false;
                let op = match read.get_reserved_op(&candidate.id, &mut |bytes| {
                    if bytes > TOPIC_PAGE_BYTES.saturating_sub(used_bytes) {
                        over_budget = true;
                        return Err(irokle::Error::SyncCapacity(format!(
                            "replication record {} exceeds the remaining {} byte page budget",
                            candidate.id,
                            TOPIC_PAGE_BYTES.saturating_sub(used_bytes)
                        )));
                    }
                    op_bytes = bytes;
                    Ok(())
                }) {
                    Ok(Some(op)) => op,
                    Ok(None) => {
                        return Err(irokle::Error::Storage(format!(
                            "missing op for {}",
                            candidate.id
                        )));
                    }
                    Err(_) if over_budget && !records.is_empty() => {
                        candidates.push(candidate);
                        break;
                    }
                    Err(error) => return Err(error),
                };
                used_bytes = used_bytes.saturating_add(op_bytes);
                let stored_meta = read.get_meta(&candidate.id)?.ok_or_else(|| {
                    irokle::Error::Storage(format!("missing op meta for {}", candidate.id))
                })?;
                let meta = OpMeta {
                    op_id: candidate.id,
                    actor_id: stored_meta.actor_id,
                    actor_seq: stored_meta.actor_seq,
                    observed_clock: stored_meta.observed_clock,
                };
                let record = match &op.signed.body.payload {
                    irokle::TopicPayload::Event(envelope) => {
                        match envelope.decode_event::<CraqleGraphEvent>() {
                            Ok(event) => TopicRecord::Event(EventRecord { event, meta }),
                            Err(error) => {
                                let error_kind =
                                    if matches!(error, irokle::Error::EventTypeMismatch { .. }) {
                                        crate::CraqleErrorKind::Unsupported
                                    } else {
                                        crate::CraqleErrorKind::CorruptAuthoritativeData
                                    };
                                TopicRecord::Rejected(RejectedTopicRecord {
                                    meta,
                                    payload_digest: *blake3::hash(&envelope.payload).as_bytes(),
                                    error_kind,
                                    reason: if error_kind == crate::CraqleErrorKind::Unsupported {
                                        "unsupported graph-event version or type".to_owned()
                                    } else {
                                        "malformed or poison graph-event payload".to_owned()
                                    },
                                })
                            }
                        }
                    }
                    _ => TopicRecord::Control(meta),
                };
                records.push(record);
                if candidate.sequence < target.get(&candidate.actor) {
                    let next = next_record(
                        read,
                        &topic_id,
                        ActorPoint {
                            actor: candidate.actor,
                            after: candidate.sequence,
                        },
                    )?
                    .ok_or_else(|| {
                        irokle::Error::Storage(format!(
                            "expired replication cursor: position missing for actor {}",
                            candidate.actor
                        ))
                    })?;
                    if next.sequence > target.get(&candidate.actor) {
                        return Err(irokle::Error::Storage(format!(
                            "expired replication cursor: position missing for actor {}",
                            candidate.actor
                        )));
                    }
                    candidates.push(next);
                }
            }
            let more = !candidates.is_empty();
            state.target = more.then_some(target);
            Ok((records, state, more))
        });
        let (records, state, more) = match page {
            Err(irokle::Error::Storage(reason))
                if reason.starts_with("expired replication cursor: ") =>
            {
                return Err(CraqleSyncError::ExpiredCursor {
                    topic: topic_id,
                    reason,
                });
            }
            result => result?,
        };
        Ok(TopicCatchup {
            records,
            cursor: TopicCursor::resuming(state),
            more,
        })
    }

    fn topic_cursor_at(
        &self,
        topic_id: irokle::TopicId,
        clock: &irokle::ActorClock,
    ) -> SyncResult<Vec<u8>> {
        let view = self
            .node
            .storage()
            .topic_view(&topic_id, None)?
            .ok_or_else(|| irokle::Error::Storage(format!("missing topic state for {topic_id}")))?;
        for (actor, sequence) in clock.iter() {
            if *sequence > view.clock.get(actor) {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "replacement cursor exceeds topic history for actor {actor}"
                )));
            }
        }
        encode_topic_cursor(&TopicCursorPayload {
            version: TOPIC_CURSOR_VERSION,
            topic: topic_id,
            epoch: view.epoch,
            genesis: view.state.genesis,
            clock: clock.clone(),
            target: None,
        })
    }

    fn topic_frontier(&self, topic_id: irokle::TopicId) -> SyncResult<TopicFrontier> {
        let view = self
            .node
            .storage()
            .topic_view(&topic_id, None)?
            .ok_or_else(|| irokle::Error::Storage(format!("missing topic state for {topic_id}")))?;
        Ok(TopicFrontier {
            clock: view.clock,
            epoch: view.epoch,
            genesis: view.state.genesis,
        })
    }

    fn topic_record(
        &self,
        topic_id: irokle::TopicId,
        id: irokle::OpId,
    ) -> SyncResult<Option<TopicRecord>> {
        Ok(self.node.storage().read_snapshot(|read| {
            if read.topic_view(&topic_id, None)?.is_none() {
                return Ok(None);
            }
            let Some(stored_meta) = read.get_meta(&id)? else {
                return Ok(None);
            };
            if stored_meta.topic_id != topic_id {
                return Ok(None);
            }
            let Some(op) = read.get_reserved_op(&id, &mut |bytes| {
                if bytes > TOPIC_PAGE_BYTES {
                    return Err(irokle::Error::SyncCapacity(format!(
                        "replication record {id} exceeds the {TOPIC_PAGE_BYTES} byte limit"
                    )));
                }
                Ok(())
            })?
            else {
                return Err(irokle::Error::Storage(format!("missing op for {id}")));
            };
            if op.signed.body.topic_id != topic_id
                || op.signed.body.actor_id != stored_meta.actor_id
                || op.signed.body.actor_seq != stored_meta.actor_seq
            {
                return Err(irokle::Error::Storage(format!(
                    "op body disagrees with stored metadata for {id}"
                )));
            }
            let meta = OpMeta {
                op_id: id,
                actor_id: stored_meta.actor_id,
                actor_seq: stored_meta.actor_seq,
                observed_clock: stored_meta.observed_clock,
            };
            let record = match &op.signed.body.payload {
                irokle::TopicPayload::Event(envelope) => {
                    match envelope.decode_event::<CraqleGraphEvent>() {
                        Ok(event) => TopicRecord::Event(EventRecord { event, meta }),
                        Err(error) => {
                            let error_kind =
                                if matches!(error, irokle::Error::EventTypeMismatch { .. }) {
                                    crate::CraqleErrorKind::Unsupported
                                } else {
                                    crate::CraqleErrorKind::CorruptAuthoritativeData
                                };
                            TopicRecord::Rejected(RejectedTopicRecord {
                                meta,
                                payload_digest: *blake3::hash(&envelope.payload).as_bytes(),
                                error_kind,
                                reason: if error_kind == crate::CraqleErrorKind::Unsupported {
                                    "unsupported graph-event version or type".to_owned()
                                } else {
                                    "malformed or poison graph-event payload".to_owned()
                                },
                            })
                        }
                    }
                }
                _ => TopicRecord::Control(meta),
            };
            Ok(Some(record))
        })?)
    }

    fn history_snapshot(&self, request: &HistoryRequest) -> SyncResult<HistorySnapshot> {
        let view = self
            .node
            .storage()
            .topic_view(&request.topic, None)?
            .ok_or_else(|| {
                irokle::Error::Storage(format!("missing topic state for {}", request.topic))
            })?;
        if request.target != view.clock {
            return Err(CraqleSyncError::InvalidEvent(
                "history repair target is not the current branch frontier".to_owned(),
            ));
        }
        for (actor, sequence) in request.target.iter() {
            if *sequence > view.clock.get(actor) {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "history target exceeds stored topic position for actor {actor}"
                )));
            }
        }
        let initial = TopicCursorPayload {
            version: TOPIC_CURSOR_VERSION,
            topic: request.topic,
            epoch: view.epoch,
            genesis: view.state.genesis,
            clock: irokle::ActorClock::default(),
            target: Some(request.target.clone()),
        };
        let mut cursor = Some(encode_topic_cursor(&initial)?);
        let mut state = HistoryBuild {
            snapshot: crate::GraphReplicaSnapshot {
                graph: request.graph.clone(),
                clock: VectorClock::default(),
                quads: Vec::new(),
            },
            bytes: 0,
        };
        loop {
            let catchup = self.topic_records_since(request.topic, cursor.as_deref())?;
            let TopicCatchup {
                records,
                cursor: mut progress,
                more,
            } = catchup;
            for record in &records {
                match record {
                    TopicRecord::Rejected(record) => {
                        return Err(CraqleSyncError::InvalidEvent(format!(
                            "authoritative history contains rejected record {}",
                            record.meta.op_id
                        )));
                    }
                    TopicRecord::Control(_) => {}
                    TopicRecord::Event(record) => {
                        if apply_history(&mut state, record)? {
                            return Ok(HistorySnapshot::Tombstoned);
                        }
                    }
                }
                progress.consume(record);
            }
            if !more {
                for quad in &mut state.snapshot.quads {
                    quad.dots
                        .sort_unstable_by_key(|dot| (dot.actor, dot.counter));
                }
                state.snapshot.quads.sort_unstable_by(|left, right| {
                    (&left.subject, &left.predicate, &left.object).cmp(&(
                        &right.subject,
                        &right.predicate,
                        &right.object,
                    ))
                });
                return Ok(HistorySnapshot::Live(state.snapshot));
            }
            cursor = progress.encode()?;
            if cursor.is_none() {
                return Err(CraqleSyncError::InvalidEvent(
                    "history page made no cursor progress".to_owned(),
                ));
            }
        }
    }

    fn find_mutation(
        &self,
        receipt: &MutationReceipt,
    ) -> SyncResult<Option<EventRecord<CraqleGraphEvent>>> {
        let (Some(topic), Some(after), Some(epoch), Some(genesis)) = (
            receipt.topic,
            receipt.publish_after.as_ref(),
            receipt.topic_epoch,
            receipt.topic_genesis,
        ) else {
            return Ok(None);
        };
        let view = self
            .node
            .storage()
            .topic_view(&topic, None)?
            .ok_or_else(|| irokle::Error::Storage(format!("missing topic state for {topic}")))?;
        if view.epoch != epoch || view.state.genesis != genesis {
            return Err(CraqleSyncError::ExpiredCursor {
                topic,
                reason: "prepared mutation belongs to an expired topic branch".to_owned(),
            });
        }
        if let Some(event_id) = receipt.event_id {
            let id = irokle::OpId::from_bytes(event_id);
            return match self.topic_record(topic, id)? {
                Some(TopicRecord::Event(record))
                    if matches!(
                        &record.event,
                        CraqleGraphEvent::Mutation { id, .. } if *id == receipt.id
                    ) =>
                {
                    Ok(Some(record))
                }
                Some(_) => Err(CraqleSyncError::InvalidEvent(
                    "receipt event does not carry its stable mutation id".to_owned(),
                )),
                None => Err(CraqleSyncError::ExpiredCursor {
                    topic,
                    reason: "receipt event is no longer available".to_owned(),
                }),
            };
        }
        let initial = TopicCursorPayload {
            version: TOPIC_CURSOR_VERSION,
            topic,
            epoch,
            genesis,
            clock: after.clone(),
            target: Some(view.clock),
        };
        let mut cursor = Some(encode_topic_cursor(&initial)?);
        let local = irokle::actor_id_for(topic, self.node.peer_id());
        loop {
            let catchup = self.topic_records_since(topic, cursor.as_deref())?;
            let TopicCatchup {
                records,
                cursor: mut progress,
                more,
            } = catchup;
            for record in &records {
                match record {
                    TopicRecord::Event(record)
                        if matches!(
                            &record.event,
                            CraqleGraphEvent::Mutation { id, .. } if *id == receipt.id
                        ) =>
                    {
                        return Ok(Some(record.clone()));
                    }
                    // Only this node publishes its prepared mutation, so peer rejections cannot hide it.
                    TopicRecord::Rejected(record) if record.meta.actor_id == local => {
                        return Err(CraqleSyncError::InvalidEvent(format!(
                            "prepared mutation search reached rejected record {}",
                            record.meta.op_id
                        )));
                    }
                    _ => progress.consume(record),
                }
            }
            if !more {
                return Ok(None);
            }
            cursor = progress.encode()?;
            if cursor.is_none() {
                return Err(CraqleSyncError::InvalidEvent(
                    "prepared mutation search made no cursor progress".to_owned(),
                ));
            }
        }
    }

    fn is_local_record(
        &self,
        topic_id: irokle::TopicId,
        record: &EventRecord<CraqleGraphEvent>,
    ) -> bool {
        record.meta.actor_id == irokle::actor_id_for(topic_id, self.node.peer_id())
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

/// Convert an event to a [`Batch`] without changing its causally delivered operation order.
fn batch_from_changes<I>(cx: EventBatchCtx<'_>, changes: I) -> SyncResult<Batch>
where
    I: IntoIterator<Item = MaterializedQuadChange>,
{
    let EventBatchCtx { graph, meta } = cx;
    Batch::from_changes(
        graph.clone(),
        actor_from_irokle(meta.actor_id),
        meta.actor_seq,
        clock_from_irokle(&meta.observed_clock),
        changes,
        Utc::now(),
    )
    .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))
}

/// Largest term craqle accepts from a topic. Well past any real IRI or literal,
/// and small enough that one record cannot be an allocation attack.
pub(crate) const MAX_TERM_BYTES: usize = 4 * 1024 * 1024;

/// Aggregate limits for one record or snapshot. A per-term cap bounds a single
/// string, never the total work an envelope can demand.
const MAX_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENVELOPE_ROWS: usize = 1 << 20;
const MAX_ENVELOPE_DOTS: usize = 1 << 20;
const MAX_ENVELOPE_ACTORS: usize = 1 << 16;

/// Which RDF term form an encoded string holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TermShape {
    Iri,
    Blank,
    Literal,
}

/// A quad position, with the term forms RDF allows there.
#[derive(Clone, Copy)]
enum Place {
    Subject,
    Predicate,
    Object,
}

impl Place {
    fn allows(self, shape: TermShape) -> bool {
        match self {
            Self::Subject => shape != TermShape::Literal,
            Self::Predicate => shape == TermShape::Iri,
            Self::Object => true,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Subject => "subject",
            Self::Predicate => "predicate",
            Self::Object => "object",
        }
    }
}

const PLACES: [Place; 3] = [Place::Subject, Place::Predicate, Place::Object];

fn rejected(text: &str) -> CraqleSyncError {
    CraqleSyncError::InvalidEvent(format!(
        "term `{}` is not a complete encoded IRI, literal or blank node",
        text.chars().take(64).collect::<String>()
    ))
}

/// Accepts unambiguous N-Triples IRIREF bodies, including relative RO-Crate
/// entity identifiers.
fn iri_body_ok(body: &str) -> bool {
    !body.is_empty()
        && !body.chars().any(|ch| {
            ch <= ' ' || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\')
        })
}

/// N-Triples LANGTAG.
fn language_ok(tag: &str) -> bool {
    let mut parts = tag.split('-');
    let primary = parts.next().unwrap_or_default();
    !primary.is_empty()
        && primary.chars().all(|ch| ch.is_ascii_alphabetic())
        && parts.all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_alphanumeric()))
}

/// Consume an N-Triples literal completely: a quoted value with legal escapes,
/// then an optional language tag or datatype IRI and nothing after it.
fn literal_ok(text: &str) -> bool {
    let Some(mut rest) = text.strip_prefix('"') else {
        return false;
    };
    loop {
        let Some(next) = rest.chars().next() else {
            return false;
        };
        rest = &rest[next.len_utf8()..];
        match next {
            '"' => break,
            '\\' => {
                let Some(escape) = rest.chars().next() else {
                    return false;
                };
                rest = &rest[escape.len_utf8()..];
                let width = match escape {
                    't' | 'b' | 'n' | 'r' | 'f' | '"' | '\'' | '\\' => 0,
                    'u' => 4,
                    'U' => 8,
                    _ => return false,
                };
                if rest.len() < width
                    || !rest.is_char_boundary(width)
                    || !rest[..width].chars().all(|ch| ch.is_ascii_hexdigit())
                {
                    return false;
                }
                rest = &rest[width..];
            }
            _ => {}
        }
    }
    if rest.is_empty() {
        return true;
    }
    if let Some(tag) = rest.strip_prefix('@') {
        return language_ok(tag);
    }
    match rest
        .strip_prefix("^^<")
        .and_then(|iri| iri.strip_suffix('>'))
    {
        Some(datatype) => iri_body_ok(datatype),
        None => false,
    }
}

/// Parses a complete term and its valid quad positions; delimiter prefixes
/// alone do not prove that an RDF reader accepts the term.
fn check_term(term: &EncodedTerm) -> SyncResult<TermShape> {
    let text = term.0.as_str();
    if term.is_rdf_star() {
        return Err(CraqleSyncError::InvalidEvent(format!(
            "UnsupportedRdfStarTerm: `{text}`"
        )));
    }
    if text.len() > MAX_TERM_BYTES {
        return Err(CraqleSyncError::InvalidEvent(format!(
            "term of {} bytes exceeds the {MAX_TERM_BYTES} byte limit",
            text.len()
        )));
    }
    if let Some(body) = text
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
    {
        if iri_body_ok(body) {
            return Ok(TermShape::Iri);
        }
    } else if let Some(label) = text.strip_prefix("_:") {
        if oxrdf::BlankNode::new(label).is_ok() {
            return Ok(TermShape::Blank);
        }
    } else if text.starts_with('"') && literal_ok(text) {
        return Ok(TermShape::Literal);
    }
    Err(rejected(text))
}

/// Every dot a claimed context holds that `known` does not cover.
pub(crate) fn missing_dots(claim: &VectorClock, known: &VectorClock) -> Vec<Dot> {
    claim
        .0
        .iter()
        .map(|(&actor, &counter)| Dot { actor, counter })
        .filter(|dot| !known.contains(dot))
        .collect()
}

/// Running checks for one record or snapshot: each distinct term is parsed
/// once, and the totals a per-term cap cannot bound are accumulated.
struct Envelope<'a> {
    shapes: HashMap<&'a str, TermShape>,
    actors: HashSet<ActorId>,
    bytes: usize,
    rows: usize,
    dots: usize,
}

impl<'a> Envelope<'a> {
    fn new() -> Self {
        Self {
            shapes: HashMap::new(),
            actors: HashSet::new(),
            bytes: 0,
            rows: 0,
            dots: 0,
        }
    }

    /// Validate one quad's three terms in their own positions.
    fn quad(&mut self, terms: [&'a EncodedTerm; 3]) -> SyncResult<()> {
        self.rows += 1;
        if self.rows > MAX_ENVELOPE_ROWS {
            return Err(CraqleSyncError::InvalidEvent(format!(
                "envelope exceeds the {MAX_ENVELOPE_ROWS} row limit"
            )));
        }
        for (term, place) in terms.into_iter().zip(PLACES) {
            self.bytes = self.bytes.saturating_add(term.0.len());
            if self.bytes > MAX_ENVELOPE_BYTES {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "envelope exceeds the {MAX_ENVELOPE_BYTES} byte limit"
                )));
            }
            let shape = match self.shapes.get(term.0.as_str()) {
                Some(shape) => *shape,
                None => {
                    let shape = check_term(term)?;
                    self.shapes.insert(term.0.as_str(), shape);
                    shape
                }
            };
            if !place.allows(shape) {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "term `{}` is not a legal RDF {}",
                    term.0.chars().take(64).collect::<String>(),
                    place.label()
                )));
            }
        }
        Ok(())
    }

    /// Count the actors one declared context names.
    fn clock(&mut self, clock: &VectorClock) -> SyncResult<()> {
        self.actors.extend(clock.0.keys().copied());
        self.limit()
    }

    /// Count one dot set.
    fn dots(&mut self, dots: &[Dot]) -> SyncResult<()> {
        self.dots = self.dots.saturating_add(dots.len());
        if self.dots > MAX_ENVELOPE_DOTS {
            return Err(CraqleSyncError::InvalidEvent(format!(
                "envelope exceeds the {MAX_ENVELOPE_DOTS} dot limit"
            )));
        }
        self.actors.extend(dots.iter().map(|dot| dot.actor));
        self.limit()
    }

    fn limit(&self) -> SyncResult<()> {
        if self.actors.len() > MAX_ENVELOPE_ACTORS {
            return Err(CraqleSyncError::InvalidEvent(format!(
                "envelope exceeds the {MAX_ENVELOPE_ACTORS} actor limit"
            )));
        }
        Ok(())
    }
}

/// A graph name keys every row of the graph, so it must itself be a complete
/// encoded IRI that any peer can reproduce.
fn check_graph(graph: &GraphId) -> SyncResult<()> {
    check_term(&EncodedTerm::from_named_node(&graph.0)).map(|_| ())
}

/// Validate every term a record carries before any of it reaches the store, so
/// content a retry could never accept is rejected here.
fn check_changes(changes: &[MaterializedQuadChange]) -> SyncResult<()> {
    let mut envelope = Envelope::new();
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
        envelope.quad(terms)?;
    }
    Ok(())
}

/// Validates an external batch: adds carry its event and removes witness only
/// the declared base clock.
pub(crate) fn check_batch(batch: &Batch) -> SyncResult<()> {
    check_graph(&batch.graph)?;
    let mut envelope = Envelope::new();
    envelope.clock(&batch.base_clock)?;
    let identity = Dot {
        actor: batch.actor,
        counter: batch.counter,
    };
    for op in &batch.ops {
        match op {
            QuadOp::Add {
                subject,
                predicate,
                object,
                dot,
            } => {
                envelope.quad([subject, predicate, object])?;
                envelope.dots(std::slice::from_ref(dot))?;
                // One event dot may cover several quads of the same batch, but
                // it is always that batch's own event.
                if *dot != identity {
                    return Err(CraqleSyncError::InvalidEvent(format!(
                        "add dot {}:{} does not match the batch event {}:{}",
                        dot.actor, dot.counter, identity.actor, identity.counter
                    )));
                }
            }
            QuadOp::Remove {
                subject,
                predicate,
                object,
                witnessed,
            } => {
                envelope.quad([subject, predicate, object])?;
                envelope.clock(witnessed)?;
                if let Some(dot) = missing_dots(witnessed, &batch.base_clock).first() {
                    return Err(CraqleSyncError::InvalidEvent(format!(
                        "remove witnessed {}:{} beyond the batch base clock",
                        dot.actor, dot.counter
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Rejects application snapshots with out-of-context dots, repeated quads, or
/// repeated dots because no replica can hold that state.
pub(crate) fn check_snapshot(snapshot: &crate::GraphReplicaSnapshot) -> SyncResult<()> {
    check_graph(&snapshot.graph)?;
    let mut envelope = Envelope::new();
    envelope.clock(&snapshot.clock)?;
    let mut keys = HashSet::new();
    for quad in &snapshot.quads {
        envelope.quad([&quad.subject, &quad.predicate, &quad.object])?;
        envelope.dots(&quad.dots)?;
        if quad.dots.is_empty() {
            return Err(CraqleSyncError::InvalidEvent(
                "snapshot holds a live quad with no dot".to_string(),
            ));
        }
        if !keys.insert((&quad.subject.0, &quad.predicate.0, &quad.object.0)) {
            return Err(CraqleSyncError::InvalidEvent(
                "snapshot repeats a quad identity".to_string(),
            ));
        }
        let mut seen = HashSet::with_capacity(quad.dots.len());
        for dot in &quad.dots {
            if !seen.insert(*dot) {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "snapshot repeats dot {}:{}",
                    dot.actor, dot.counter
                )));
            }
            if !snapshot.clock.contains(dot) {
                return Err(CraqleSyncError::InvalidEvent(format!(
                    "live dot {}:{} is not covered by the snapshot clock",
                    dot.actor, dot.counter
                )));
            }
        }
    }
    Ok(())
}

/// Borrowing variant, for callers that only hold a reference to the record
/// (catch-up and reconcile replay both re-read their records afterwards).
pub(crate) fn batch_from_record(
    record: &EventRecord<CraqleGraphEvent>,
) -> SyncResult<Option<ReplicatedGraphMutation>> {
    let (graph, changes, render_hints, mutation_id) = match &record.event {
        CraqleGraphEvent::QuadChanges { graph, changes } => {
            (graph, changes, None, MutationId::from_op(record.meta.op_id))
        }
        CraqleGraphEvent::RoCrateMutation {
            graph,
            changes,
            context,
            license,
            license_digest,
            tag,
        } => (
            graph,
            changes,
            Some(TaggedRenderHints {
                hints: RenderHints {
                    context: context.clone(),
                    license: license.clone(),
                    license_digest: *license_digest,
                },
                tag: *tag,
            }),
            MutationId::from_op(record.meta.op_id),
        ),
        CraqleGraphEvent::Mutation {
            id,
            graph,
            changes,
            render_hints,
        } => (graph, changes, render_hints.clone().map(Into::into), *id),
        _ => return Ok(None),
    };
    check_changes(changes)?;
    let request_digest = request_digest(graph, changes, render_hints.as_ref())?;
    let cx = EventBatchCtx {
        graph,
        meta: &record.meta,
    };
    Ok(Some(ReplicatedGraphMutation {
        batch: batch_from_changes(cx, changes.iter().cloned())?,
        render_hints,
        mutation_id,
        request_digest,
        event_id: record.meta.op_id,
    }))
}

/// Consuming variant: moves every term string out of the record instead of
/// cloning it, for callers that drop the record right after.
pub(crate) fn batch_from_owned(
    record: EventRecord<CraqleGraphEvent>,
) -> SyncResult<Option<ReplicatedGraphMutation>> {
    let EventRecord { event, meta } = record;
    let event_id = meta.op_id;
    let (graph, changes, render_hints, mutation_id) = match event {
        CraqleGraphEvent::QuadChanges { graph, changes } => {
            (graph, changes, None, MutationId::from_op(event_id))
        }
        CraqleGraphEvent::RoCrateMutation {
            graph,
            changes,
            context,
            license,
            license_digest,
            tag,
        } => (
            graph,
            changes,
            Some(TaggedRenderHints {
                hints: RenderHints {
                    context,
                    license,
                    license_digest,
                },
                tag,
            }),
            MutationId::from_op(event_id),
        ),
        CraqleGraphEvent::Mutation {
            id,
            graph,
            changes,
            render_hints,
        } => (graph, changes, render_hints.map(Into::into), id),
        _ => return Ok(None),
    };
    check_changes(&changes)?;
    let request_digest = request_digest(&graph, &changes, render_hints.as_ref())?;
    let cx = EventBatchCtx {
        graph: &graph,
        meta: &meta,
    };
    Ok(Some(ReplicatedGraphMutation {
        batch: batch_from_changes(cx, changes)?,
        render_hints,
        mutation_id,
        request_digest,
        event_id,
    }))
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

struct HistoryBuild {
    snapshot: crate::GraphReplicaSnapshot,
    bytes: usize,
}

fn apply_history(
    state: &mut HistoryBuild,
    record: &EventRecord<CraqleGraphEvent>,
) -> SyncResult<bool> {
    let snapshot = &mut state.snapshot;
    if record.event.graph() != &snapshot.graph {
        return Err(CraqleSyncError::InvalidEvent(
            "history record targets a graph outside its topic binding".to_owned(),
        ));
    }
    if matches!(&record.event, CraqleGraphEvent::GraphDeleted { .. }) {
        return Ok(true);
    }
    let Some(mutation) = batch_from_record(record)? else {
        return Ok(false);
    };
    for op in &mutation.batch.ops {
        let (subject, predicate, object) = match op {
            QuadOp::Add {
                subject,
                predicate,
                object,
                ..
            }
            | QuadOp::Remove {
                subject,
                predicate,
                object,
                ..
            } => (subject, predicate, object),
        };
        let index = snapshot.quads.iter().position(|quad| {
            quad.subject == *subject && quad.predicate == *predicate && quad.object == *object
        });
        match op {
            QuadOp::Add { dot, .. } => {
                if let Some(index) = index {
                    if !snapshot.quads[index].dots.contains(dot) {
                        if state.bytes.saturating_add(40) > MAX_ENVELOPE_BYTES {
                            return Err(CraqleSyncError::InvalidEvent(
                                "authoritative graph history exceeds the byte limit".to_owned(),
                            ));
                        }
                        snapshot.quads[index].dots.push(*dot);
                        state.bytes += 40;
                    }
                } else {
                    let added = subject
                        .0
                        .len()
                        .saturating_add(predicate.0.len())
                        .saturating_add(object.0.len())
                        .saturating_add(40);
                    if snapshot.quads.len() >= MAX_ENVELOPE_ROWS
                        || state.bytes.saturating_add(added) > MAX_ENVELOPE_BYTES
                    {
                        return Err(CraqleSyncError::InvalidEvent(
                            "authoritative graph history exceeds the row or byte limit".to_owned(),
                        ));
                    }
                    snapshot.quads.push(crate::SnapshotQuadState {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: object.clone(),
                        dots: vec![*dot],
                    });
                    state.bytes += added;
                }
            }
            QuadOp::Remove { witnessed, .. } => {
                if let Some(index) = index {
                    let before = snapshot.quads[index].dots.len();
                    snapshot.quads[index]
                        .dots
                        .retain(|dot| !witnessed.contains(dot));
                    state.bytes = state
                        .bytes
                        .saturating_sub((before - snapshot.quads[index].dots.len()) * 40);
                    if snapshot.quads[index].dots.is_empty() {
                        state.bytes = state
                            .bytes
                            .saturating_sub(subject.0.len() + predicate.0.len() + object.0.len());
                        snapshot.quads.swap_remove(index);
                    }
                }
            }
        }
    }
    snapshot
        .clock
        .advance(mutation.batch.actor, mutation.batch.counter);
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_hides_repairs() {
        let graph = GraphId::new("urn:test:receipt:visible");
        let hidden = GraphId::new("urn:test:receipt:hidden");
        let internal = MutationReceipt {
            id: MutationId([1; 32]),
            admission_sequence: 7,
            graph: graph.clone(),
            request_digest: [2; 32],
            event_id: Some([3; 32]),
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: Some(9),
            repair_graphs: vec![graph.clone(), hidden],
            source: SourceOutcome::Applied,
            persistence: PersistenceOutcome::DataSynced,
            repairs: RepairState {
                diagnostics: RepairOutcome::Complete,
                shacl: RepairOutcome::Pending,
                search: RepairOutcome::Complete,
                query_view: RepairOutcome::Complete,
            },
            source_version: [4; 32],
            updated_unix_nanos: 11,
        };

        let outbound = internal.clone().outbound();
        assert_eq!(internal.repair_graphs.len(), 2);
        assert_eq!(outbound.repair_graphs, vec![graph]);
    }

    #[test]
    fn causal_candidates_order() {
        let first = RecordCandidate {
            generation: 3,
            actor: irokle::ActorId::from_bytes([9; 32]),
            sequence: 7,
            id: irokle::OpId::from_bytes([2; 32]),
        };
        let dependency = RecordCandidate {
            generation: 2,
            actor: irokle::ActorId::from_bytes([8; 32]),
            sequence: 4,
            id: irokle::OpId::from_bytes([3; 32]),
        };
        assert!(dependency < first);
    }

    #[test]
    fn rejects_invalid_cursors() {
        let topic = irokle::TopicId::from_bytes([1; 32]);
        let other = irokle::TopicId::from_bytes([2; 32]);
        let payload = TopicCursorPayload {
            version: TOPIC_CURSOR_VERSION,
            topic,
            epoch: 3,
            genesis: irokle::OpId::from_bytes([4; 32]),
            clock: irokle::ActorClock::default(),
            target: None,
        };
        let encoded = encode_topic_cursor(&payload).unwrap();

        assert!(matches!(
            decode_topic_cursor(topic, &[0xff]),
            Err(CraqleSyncError::CorruptCursor { .. })
        ));
        assert!(matches!(
            decode_topic_cursor(topic, &encoded[..encoded.len() - 1]),
            Err(CraqleSyncError::CorruptCursor { .. })
        ));
        assert!(matches!(
            decode_topic_cursor(other, &encoded),
            Err(CraqleSyncError::CorruptCursor { .. })
        ));

        let mut checksum_invalid = encoded.clone();
        let last = checksum_invalid.last_mut().unwrap();
        *last ^= 1;
        assert!(matches!(
            decode_topic_cursor(topic, &checksum_invalid),
            Err(CraqleSyncError::CorruptCursor { .. })
        ));

        let payload = TopicCursorPayload {
            version: TOPIC_CURSOR_VERSION + 1,
            topic,
            epoch: 3,
            genesis: irokle::OpId::from_bytes([4; 32]),
            clock: irokle::ActorClock::default(),
            target: None,
        };
        let payload_bytes = postcard::to_allocvec(&payload).unwrap();
        let future = postcard::to_allocvec(&TopicCursorEnvelope {
            payload,
            checksum: *blake3::hash(&payload_bytes).as_bytes(),
        })
        .unwrap();
        assert!(matches!(
            decode_topic_cursor(topic, &future),
            Err(CraqleSyncError::CorruptCursor { .. })
        ));

        let payload = LegacyCursorPayload {
            version: 1,
            topic,
            clock: irokle::ActorClock::default(),
        };
        let payload_bytes = postcard::to_allocvec(&payload).unwrap();
        let legacy = postcard::to_allocvec(&LegacyCursorEnvelope {
            payload,
            checksum: *blake3::hash(&payload_bytes).as_bytes(),
        })
        .unwrap();
        assert!(matches!(
            decode_topic_cursor(topic, &legacy),
            Err(CraqleSyncError::ExpiredCursor { .. })
        ));
    }
}
