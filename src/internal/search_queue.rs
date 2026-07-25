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

/// A durable FTS queue entry, tagged with the dirty token it was enqueued at.
pub(crate) trait QueueEntry {
    fn token(&self) -> u64;
}

impl QueueEntry for (GraphId, u64) {
    fn token(&self) -> u64 {
        self.1
    }
}

impl QueueEntry for (GraphId, TermId, u64) {
    fn token(&self) -> u64 {
        self.2
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
    let Some(max_token) = bound.max_token else {
        return drain(bound.chunk);
    };

    let mut chunk = bound.chunk;
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
    fn queue(tokens: &[u64]) -> impl Fn(usize) -> Result<Vec<(GraphId, u64)>> + '_ {
        move |chunk| {
            Ok(tokens
                .iter()
                .take(chunk)
                .map(|&token| (GraphId::new("urn:test:queue"), token))
                .collect())
        }
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
        assert_eq!(vec![7, 3], drained.iter().map(|e| e.1).collect::<Vec<_>>());
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
