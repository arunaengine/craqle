use crate::core::{Batch, GraphId, GraphPolicy, GraphReplicaCompactSnapshot, GraphReplicaSnapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage {
    Batch(Batch),
    Snapshot {
        snapshot: GraphReplicaSnapshot,
        policy: GraphPolicy,
    },
    CompactSnapshot {
        snapshot: GraphReplicaCompactSnapshot,
        policy: GraphPolicy,
    },
    Policy {
        graph: GraphId,
        policy: GraphPolicy,
    },
}
