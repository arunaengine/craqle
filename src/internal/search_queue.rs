//! Bounded draining of the durable FTS queues.
//!
//! Shared by the real index and the `search`-disabled stub so both honour the
//! same flush contract.

use crate::core::GraphId;
use crate::store::{Result, TermId};

/// Bounds one drain pass over the durable FTS queues.
pub struct QueueBound {
    /// Maximum entries to read from the store per drain.
    pub chunk: usize,
    /// Highest dirty token to process, or `None` to process everything queued.
    ///
    /// A bounded flush pins this to the token observed when the flush started,
    /// so a writer that keeps enqueueing cannot keep the drain alive forever.
    pub max_token: Option<u64>,
}

/// The dirty tokens one coalesced queue entry carries.
///
/// Two are needed because they answer different questions. `oldest` is the
/// token of the first enqueue that has not been indexed yet, so it decides
/// whether a bounded flush owes this entry; keeping it is what stops a later
/// enqueue from lifting pre-flush work above the bound. `latest` advances on
/// every enqueue, so an acknowledgement can tell an entry it fully covered
/// from one re-dirtied while it was reading the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirtyTokens {
    pub oldest: u64,
    pub latest: u64,
}

/// A queued subject: one dirty entry in the per-`(graph, subject)` queue.
#[derive(Clone, Debug)]
pub struct DirtySubject {
    pub graph: GraphId,
    pub subject: TermId,
    pub tokens: DirtyTokens,
}

/// A queued whole-graph entry: a reindex or a search-delete.
#[derive(Clone, Debug)]
pub struct DirtyGraph {
    pub graph: GraphId,
    pub tokens: DirtyTokens,
}

/// A durable FTS queue entry, tagged with the dirty tokens it accumulated.
pub(crate) trait QueueEntry {
    fn token(&self) -> u64;
}

impl QueueEntry for DirtyGraph {
    fn token(&self) -> u64 {
        self.tokens.oldest
    }
}

impl QueueEntry for DirtySubject {
    fn token(&self) -> u64 {
        self.tokens.oldest
    }
}

/// Drain one durable queue, keeping only entries at or below `bound.max_token`.
///
/// Queue keys are ordered by graph/subject hash rather than by token, so a
/// bounded drain can come back holding nothing but entries enqueued after the
/// bound was taken while eligible ones sit further along the scan. Widen the
/// chunk until eligible entries appear or the whole queue has been seen:
/// returning early would leave pre-flush work unindexed and break the bounded
/// flush contract ("everything enqueued before the call is indexed").
///
/// Terminates because `chunk` grows until the drain returns fewer entries than
/// requested, which means the whole queue was scanned.
pub(crate) fn drain_upto<T, D>(bound: &QueueBound, drain: D) -> Result<Vec<T>>
where
    T: QueueEntry,
    D: Fn(usize) -> Result<Vec<T>>,
{
    // Widening multiplies, so a zero chunk would never grow and never see the
    // whole queue: the loop below would spin forever.
    let mut chunk = bound.chunk.max(1);

    let Some(max_token) = bound.max_token else {
        return drain(chunk);
    };

    loop {
        let drained = drain(chunk)?;
        let whole_queue_seen = drained.len() < chunk;
        let eligible: Vec<T> = drained
            .into_iter()
            .filter(|entry| entry.token() <= max_token)
            .collect();
        if !eligible.is_empty() || whole_queue_seen {
            return Ok(eligible);
        }
        chunk = chunk.saturating_mul(4);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A queue whose scan order puts freshly enqueued entries first, which is
    /// what a hash-ordered key space does to a bounded drain.
    fn queue(tokens: &[u64]) -> impl Fn(usize) -> Result<Vec<DirtyGraph>> + '_ {
        move |chunk| {
            Ok(tokens
                .iter()
                .take(chunk)
                .map(|&token| DirtyGraph {
                    graph: GraphId::new("urn:test:queue"),
                    tokens: DirtyTokens {
                        oldest: token,
                        latest: token,
                    },
                })
                .collect())
        }
    }

    fn oldest(drained: &[DirtyGraph]) -> Vec<u64> {
        drained.iter().map(|entry| entry.tokens.oldest).collect()
    }

    #[test]
    fn drain_widens_chunk() {
        let bound = QueueBound {
            chunk: 4,
            max_token: Some(10),
        };

        let drained = drain_upto(&bound, queue(&[99, 99, 99, 99, 7, 3])).unwrap();

        // Reading one chunk would have found nothing and reported the queue
        // drained, leaving tokens 7 and 3 unindexed past a bounded flush.
        assert_eq!(vec![7, 3], oldest(&drained));
    }

    #[test]
    fn drain_returns_empty() {
        let bound = QueueBound {
            chunk: 2,
            max_token: Some(10),
        };

        let drained = drain_upto(&bound, queue(&[99, 99, 99])).unwrap();

        assert!(drained.is_empty());
    }

    /// A zero chunk must still make progress: widening it by multiplication
    /// leaves it at zero, so the drain would loop without ever reading.
    #[test]
    fn drain_clamps_chunk() {
        let bound = QueueBound {
            chunk: 0,
            max_token: Some(10),
        };

        let drained = drain_upto(&bound, queue(&[7])).unwrap();

        assert_eq!(vec![7], oldest(&drained));
    }

    #[test]
    fn drain_reads_chunk() {
        let bound = QueueBound {
            chunk: 2,
            max_token: None,
        };

        let drained = drain_upto(&bound, queue(&[99, 99, 7])).unwrap();

        assert_eq!(2, drained.len());
    }
}
