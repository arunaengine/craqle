//! Bounded draining of the durable FTS queues.
//!
//! Shared by the real index and the `search`-disabled stub so both honour the
//! same flush contract.

use crate::core::GraphId;
use crate::store::{Result, TermId};

/// Bounds one drain pass over the durable FTS queues.
pub struct QueueBound {
    /// Maximum eligible entries one drain may hand back.
    pub chunk: usize,
    /// Highest dirty token to process, or `None` to process everything queued.
    ///
    /// A bounded flush pins this to the token observed when the flush started,
    /// so a writer that keeps enqueueing cannot keep the drain alive forever.
    pub max_token: Option<u64>,
}

/// Storage rows one scan may return per requested entry while it skips queue
/// entries above `QueueBound::max_token`.
///
/// Derived from `chunk` rather than configured separately so every caller gets
/// the same bound. `chunk` caps what the caller has to prepare and hold; this
/// caps what the scan underneath it materializes on the way there.
const ROW_BUDGET_FACTOR: usize = 4;

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

/// What one drain pass covered and what it still owes.
#[derive(Debug, Default)]
pub struct DrainProgress {
    /// Queue entries committed to the index and then acknowledged.
    pub covered: usize,
    /// Eligible work is still queued under the same bound, so the caller owes
    /// a continuation pass. Distinct from `covered == 0`, which on its own
    /// only says this pass moved nothing.
    pub remaining: bool,
    /// Entries this pass could not index. They stay queued.
    pub failures: Vec<DrainFailure>,
    /// Token a recovery raised the flush target to, if one was scheduled.
    pub recovery: Option<u64>,
}

/// One queue entry that failed, with what is known about why.
#[derive(Debug, Clone)]
pub struct DrainFailure {
    pub graph: GraphId,
    pub attempts: u32,
    pub diagnostic: String,
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

/// What one bounded drain pass found.
pub(crate) struct DrainSlice<T> {
    /// Eligible entries, never more than [`QueueBound::chunk`] of them.
    pub entries: Vec<T>,
    /// Storage rows the scan returned, summed over every widening step.
    /// Reported so a test can hold the scan to its budget.
    #[allow(dead_code)]
    pub rows_read: usize,
    /// Eligible work is still queued under the same bound.
    ///
    /// Distinct from an empty `entries`: that means nothing eligible is owed,
    /// while this means the row or chunk budget stopped the scan first and the
    /// caller owes a continuation pass.
    pub remaining: bool,
}

impl<T> DrainSlice<T> {
    /// Borrow the eligible entries. Used by queue tests in other modules.
    #[allow(dead_code)]
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.entries.iter()
    }
}

/// Drain one durable queue, keeping only entries at or below `bound.max_token`.
///
/// Queue keys are ordered by graph and subject identity rather than by token,
/// so a bounded drain can come back holding nothing but entries enqueued after
/// the bound was taken while eligible ones sit further along the scan. Widen
/// the requested size until eligible entries appear, the whole queue has been
/// seen, or the row budget is spent: returning early on the first of those
/// would leave pre-flush work unindexed and break the bounded flush contract
/// ("everything enqueued before the call is indexed").
///
/// Two separate caps apply. A row budget derived from `chunk` stops a single
/// store scan from
/// materializing an unbounded slice of a queue that is mostly newer than the
/// bound, and `bound.chunk` stops the eligible entries found that way from
/// exceeding what the caller asked to prepare. Widening used to return every
/// eligible entry of the widened read, so a nominal chunk of fifty thousand
/// could hand back several times that many.
///
/// Terminates because the requested size grows until either the queue returns
/// fewer entries than requested, which means the whole queue was scanned, or
/// the row budget is reached.
pub(crate) fn drain_upto<T, D>(bound: &QueueBound, drain: D) -> Result<DrainSlice<T>>
where
    T: QueueEntry,
    D: Fn(usize) -> Result<Vec<T>>,
{
    // Widening multiplies, so a zero chunk would never grow and never see the
    // whole queue: the loop below would spin forever.
    let chunk = bound.chunk.max(1);
    let max_rows = chunk.saturating_mul(ROW_BUDGET_FACTOR).max(chunk);

    let Some(max_token) = bound.max_token else {
        let entries = drain(chunk)?;
        let rows_read = entries.len();
        let remaining = rows_read >= chunk;
        return Ok(DrainSlice {
            entries,
            rows_read,
            remaining,
        });
    };

    let mut request = chunk;
    let mut rows_read = 0usize;
    loop {
        let drained = drain(request)?;
        let whole_queue_seen = drained.len() < request;
        rows_read = rows_read.saturating_add(drained.len());

        let mut eligible: Vec<T> = drained
            .into_iter()
            .filter(|entry| entry.token() <= max_token)
            .collect();
        if !eligible.is_empty() {
            let remaining = eligible.len() > chunk;
            eligible.truncate(chunk);
            return Ok(DrainSlice {
                entries: eligible,
                rows_read,
                remaining,
            });
        }
        if whole_queue_seen {
            return Ok(DrainSlice {
                entries: Vec::new(),
                rows_read,
                remaining: false,
            });
        }
        if request >= max_rows {
            // The budget ran out before the scan reached eligible work. That
            // is owed work the caller must come back for, not an empty queue.
            return Ok(DrainSlice {
                entries: Vec::new(),
                rows_read,
                remaining: true,
            });
        }
        request = request.saturating_mul(4).min(max_rows);
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

    fn bound(chunk: usize, max_token: Option<u64>) -> QueueBound {
        QueueBound { chunk, max_token }
    }

    fn oldest(drained: &[DirtyGraph]) -> Vec<u64> {
        drained.iter().map(|entry| entry.tokens.oldest).collect()
    }

    #[test]
    fn drain_widens_chunk() {
        let drained = drain_upto(&bound(4, Some(10)), queue(&[99, 99, 99, 99, 7, 3])).unwrap();

        // Reading one chunk would have found nothing and reported the queue
        // drained, leaving tokens 7 and 3 unindexed past a bounded flush.
        assert_eq!(vec![7, 3], oldest(&drained.entries));
        assert!(!drained.remaining);
    }

    #[test]
    fn drain_returns_empty() {
        let drained = drain_upto(&bound(2, Some(10)), queue(&[99, 99, 99])).unwrap();

        assert!(drained.entries.is_empty());
        assert!(
            !drained.remaining,
            "the whole queue was seen, so nothing eligible is owed"
        );
    }

    /// A zero chunk must still make progress: widening it by multiplication
    /// leaves it at zero, so the drain would loop without ever reading.
    #[test]
    fn drain_clamps_chunk() {
        let drained = drain_upto(&bound(0, Some(10)), queue(&[7])).unwrap();

        assert_eq!(vec![7], oldest(&drained.entries));
    }

    #[test]
    fn drain_reads_chunk() {
        let drained = drain_upto(&bound(2, None), queue(&[99, 99, 7])).unwrap();

        assert_eq!(2, drained.entries.len());
    }

    /// A bounded drain must never hand back more rows than the caller asked
    /// for: widening reads past the chunk to find eligible work, and
    /// everything it found used to be returned.
    #[test]
    fn drain_bounds_rows() {
        let request = bound(2, Some(10));

        let drained = drain_upto(&request, queue(&[99, 99, 99, 99, 99, 7, 3, 5])).unwrap();

        assert!(
            drained.entries.len() <= request.chunk,
            "returned {} rows for a chunk of {}",
            drained.entries.len(),
            request.chunk
        );
        assert!(
            drained.remaining,
            "the entries cut at the chunk are still owed"
        );
    }

    /// Widening is capped by the row budget rather than by the queue, so one
    /// pass cannot materialize an unbounded slice of a mostly-newer queue. A
    /// budget that runs out owes a continuation; it is not an empty queue.
    #[test]
    fn drain_bounds_reads() {
        let reads = std::cell::RefCell::new(Vec::new());
        let tokens: Vec<u64> = (0..64).map(|_| 99).chain([7]).collect();
        let request = bound(2, Some(10));
        let budget = request.chunk * ROW_BUDGET_FACTOR;

        let drained = drain_upto(&request, |chunk| {
            reads.borrow_mut().push(chunk);
            queue(&tokens)(chunk)
        })
        .unwrap();

        let peak = reads.borrow().iter().copied().max().unwrap_or(0);
        assert!(
            peak <= budget,
            "one scan asked for {peak} rows against a budget of {budget}"
        );
        assert!(
            drained.entries.is_empty() && drained.remaining,
            "a budget that stops short of eligible work owes a continuation"
        );
    }

    /// The same scan with room to finish must still reach the eligible entry,
    /// and stay inside the budget getting there.
    #[test]
    fn drain_finds_eligible() {
        let tokens: Vec<u64> = (0..64).map(|_| 99).chain([7]).collect();
        let request = bound(32, Some(10));
        let budget = request.chunk * ROW_BUDGET_FACTOR;

        let drained = drain_upto(&request, queue(&tokens)).unwrap();

        assert_eq!(vec![7], oldest(&drained.entries));
        assert!(
            drained.rows_read <= budget * 2,
            "read {} rows against a budget of {budget}",
            drained.rows_read
        );
    }
}
