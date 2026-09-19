//! Exactly-once ownership of query-view maintenance with first-in admission.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT
//!
//! One owner at a time plans query-view counters against the committed state and
//! publishes source rows and query-view rows in a single database batch. Waiting
//! callers queue in arrival order on a private condition variable, so nobody
//! spins and releasing wakes exactly one thread. Ownership carries a generation,
//! so a retired guard can never release a newer owner.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// A queued waiter's private wake slot. A stored generation is a direct handoff of
/// ownership from the releasing owner, so no later caller can overtake it.
struct Waiter {
    granted: Mutex<Option<u64>>,
    ready: Condvar,
}

struct GateState {
    /// Generation of the current logical owner.
    owner: Option<u64>,
    next_generation: u64,
    waiters: VecDeque<Arc<Waiter>>,
}

pub(crate) struct QvCommitGate {
    state: Mutex<GateState>,
}

impl QvCommitGate {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                owner: None,
                next_generation: 1,
                waiters: VecDeque::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes ownership for maintenance that must not wait, such as a rebuild.
    pub(crate) fn try_acquire(&self) -> Option<QvCommitOwner<'_>> {
        let mut state = self.lock();
        if state.owner.is_none() && state.waiters.is_empty() {
            return Some(self.own(&mut state));
        }
        None
    }

    /// Waits in arrival order for ownership and gives up its queue place when the
    /// deadline passes. A handoff that already arrived is honoured, not discarded.
    pub(crate) fn acquire_timeout(&self, timeout: Duration) -> Option<QvCommitOwner<'_>> {
        let waiter = {
            let mut state = self.lock();
            if state.owner.is_none() && state.waiters.is_empty() {
                return Some(self.own(&mut state));
            }
            let waiter = Arc::new(Waiter {
                granted: Mutex::new(None),
                ready: Condvar::new(),
            });
            state.waiters.push_back(Arc::clone(&waiter));
            waiter
        };
        {
            let granted = waiter
                .granted
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let (granted, _) = waiter
                .ready
                .wait_timeout_while(granted, timeout, |granted| granted.is_none())
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(generation) = *granted {
                return Some(self.owner_for(generation));
            }
        }
        let mut state = self.lock();
        let queued = state
            .waiters
            .iter()
            .position(|queued| Arc::ptr_eq(queued, &waiter));
        if let Some(queued) = queued {
            state.waiters.remove(queued);
            return None;
        }
        drop(state);
        // No longer queued, so the release path already handed ownership over.
        let granted = waiter
            .granted
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (*granted).map(|generation| self.owner_for(generation))
    }

    fn own(&self, state: &mut GateState) -> QvCommitOwner<'_> {
        let generation = state.next_generation;
        state.next_generation += 1;
        state.owner = Some(generation);
        self.owner_for(generation)
    }

    fn owner_for(&self, generation: u64) -> QvCommitOwner<'_> {
        QvCommitOwner {
            gate: self,
            generation,
            released: false,
        }
    }

    /// Releases `generation` and hands the gate to the longest-waiting caller.
    /// Releasing a generation that no longer owns the gate does nothing and
    /// reports `false`.
    fn release(&self, generation: u64) -> bool {
        let mut state = self.lock();
        if state.owner != Some(generation) {
            return false;
        }
        state.owner = None;
        if let Some(waiter) = state.waiters.pop_front() {
            let next = state.next_generation;
            state.next_generation += 1;
            state.owner = Some(next);
            let mut granted = waiter
                .granted
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *granted = Some(next);
            waiter.ready.notify_one();
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn owner_count(&self) -> usize {
        usize::from(self.lock().owner.is_some())
    }

    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.lock().waiters.len()
    }
}

/// Sole owner of query-view maintenance. Release happens exactly once, whether
/// through [`Self::finish`], an early return, or an unwind.
pub(crate) struct QvCommitOwner<'a> {
    gate: &'a QvCommitGate,
    generation: u64,
    released: bool,
}

impl QvCommitOwner<'_> {
    /// Releases ownership now instead of at the end of the enclosing scope.
    pub(crate) fn finish(mut self) {
        self.release_once();
    }

    fn release_once(&mut self) -> bool {
        if self.released {
            return false;
        }
        self.released = true;
        self.gate.release(self.generation)
    }
}

impl Drop for QvCommitOwner<'_> {
    fn drop(&mut self) {
        self.release_once();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    /// Long enough that a slow machine never turns exclusion into a timeout.
    const PATIENT: Duration = Duration::from_secs(60);

    #[test]
    fn stale_guard_cannot_release_owner() {
        let gate = QvCommitGate::new();
        let first = gate.try_acquire().expect("gate starts free");
        let stale = first.generation;
        first.finish();
        let second = gate.try_acquire().expect("gate is free again");
        // Replaying the retired generation's release must not evict the live
        // owner, which is the double-release schedule this gate removes.
        assert!(!gate.release(stale));
        assert_eq!(gate.owner_count(), 1);
        assert!(gate.try_acquire().is_none());
        drop(second);
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn owner_release_happens_once() {
        let gate = QvCommitGate::new();
        let owner = gate.try_acquire().expect("gate starts free");
        let generation = owner.generation;
        drop(owner);
        assert!(!gate.release(generation));
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn waiters_are_admitted_in_order() {
        let gate = Arc::new(QvCommitGate::new());
        let held = gate.try_acquire().expect("gate starts free");
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();
        for index in 0..4u64 {
            // Each thread is observed in the queue before the next one starts, so
            // arrival order is unambiguous without relying on thread timing.
            let waiting = Arc::clone(&gate);
            let tx = tx.clone();
            let expected = usize::try_from(index).expect("small index");
            handles.push(std::thread::spawn(move || {
                let owner = waiting.acquire_timeout(PATIENT).expect("admitted in turn");
                tx.send(index).expect("receiver outlives the senders");
                drop(owner);
            }));
            while gate.waiting() <= expected {
                std::thread::yield_now();
            }
        }
        drop(tx);
        drop(held);
        let order: Vec<u64> = rx.iter().collect();
        for handle in handles {
            handle.join().expect("waiter thread finished");
        }
        assert_eq!(order, vec![0, 1, 2, 3]);
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn release_wakes_one_waiter() {
        let gate = Arc::new(QvCommitGate::new());
        let held = gate.try_acquire().expect("gate starts free");
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let waiting = Arc::clone(&gate);
            let running = Arc::clone(&running);
            let peak = Arc::clone(&peak);
            handles.push(std::thread::spawn(move || {
                let owner = waiting.acquire_timeout(PATIENT).expect("admitted in turn");
                let active = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                running.fetch_sub(1, Ordering::SeqCst);
                drop(owner);
            }));
        }
        while gate.waiting() < 4 {
            std::thread::yield_now();
        }
        drop(held);
        for handle in handles {
            handle.join().expect("waiter thread finished");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 1, "owner maximum is one");
    }

    #[test]
    fn cancelled_waiter_frees_its_place() {
        let gate = Arc::new(QvCommitGate::new());
        let held = gate.try_acquire().expect("gate starts free");
        let cancelling = Arc::clone(&gate);
        let cancelled = std::thread::spawn(move || {
            cancelling
                .acquire_timeout(Duration::from_millis(50))
                .is_none()
        });
        while gate.waiting() < 1 {
            std::thread::yield_now();
        }
        let follower = Arc::clone(&gate);
        let (tx, rx) = mpsc::channel();
        let queued = std::thread::spawn(move || {
            let owner = follower.acquire_timeout(PATIENT).expect("admitted in turn");
            tx.send(()).expect("receiver outlives the sender");
            drop(owner);
        });
        while gate.waiting() < 2 {
            std::thread::yield_now();
        }
        assert!(cancelled.join().expect("cancelled waiter finished"));
        drop(held);
        rx.recv().expect("the surviving waiter is admitted");
        queued.join().expect("waiter thread finished");
        assert_eq!(gate.waiting(), 0);
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn handoff_before_park_survives() {
        let gate = QvCommitGate::new();
        let owner = gate.try_acquire().expect("gate starts free");
        let waiter = Arc::new(Waiter {
            granted: Mutex::new(None),
            ready: Condvar::new(),
        });
        gate.lock().waiters.push_back(Arc::clone(&waiter));
        // The notification lands before the waiter ever parks.
        drop(owner);
        let granted = waiter.granted.lock().expect("uncontended slot");
        assert!(granted.is_some());
        assert_eq!(gate.owner_count(), 1);
    }

    #[test]
    fn unwinding_owner_releases_the_gate() {
        let gate = QvCommitGate::new();
        let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = gate.try_acquire().expect("gate starts free");
            panic!("staging failed");
        }));
        assert!(attempt.is_err());
        assert_eq!(gate.owner_count(), 0);
        assert!(gate.try_acquire().is_some(), "a healthy write proceeds");
    }
}
