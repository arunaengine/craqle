//! Serializes query-view maintenance with first-in admission and generation-safe release.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// A queued waiter's private wake slot. A stored generation is a direct handoff of
/// ownership from the releasing owner, so no later caller can overtake it.
struct Waiter {
    granted: Mutex<Option<u64>>,
    ready: Condvar,
    #[cfg(test)]
    wake_checks: AtomicUsize,
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
                #[cfg(test)]
                wake_checks: AtomicUsize::new(0),
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
                .wait_timeout_while(granted, timeout, |granted| {
                    #[cfg(test)]
                    waiter.wake_checks.fetch_add(1, Ordering::SeqCst);
                    granted.is_none()
                })
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(generation) = *granted {
                return Some(self.owner_for(generation));
            }
        }
        self.remove_waiter(&waiter)
    }

    fn remove_waiter(&self, waiter: &Arc<Waiter>) -> Option<QvCommitOwner<'_>> {
        let mut state = self.lock();
        let queued = state
            .waiters
            .iter()
            .position(|queued| Arc::ptr_eq(queued, waiter));
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

    /// Releases `generation` to the longest waiter. A stale generation does
    /// nothing and reports `false`.
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
    use std::sync::mpsc;
    use std::time::Instant;

    /// Long enough that a slow machine never turns exclusion into a timeout.
    const PATIENT: Duration = Duration::from_secs(60);
    const DEADLOCK_CAP: Duration = Duration::from_secs(30);

    fn await_waiters(gate: &QvCommitGate, count: usize) {
        let deadline = Instant::now() + DEADLOCK_CAP;
        while gate.waiting() < count {
            assert!(Instant::now() < deadline, "waiter admission stalled");
            std::thread::yield_now();
        }
    }

    fn queued_waiter() -> Arc<Waiter> {
        Arc::new(Waiter {
            granted: Mutex::new(None),
            ready: Condvar::new(),
            wake_checks: AtomicUsize::new(0),
        })
    }

    fn await_thread<T>(handle: &std::thread::JoinHandle<T>) {
        let deadline = Instant::now() + DEADLOCK_CAP;
        while !handle.is_finished() {
            assert!(Instant::now() < deadline, "worker did not finish");
            std::thread::yield_now();
        }
    }

    #[test]
    fn stale_release_ignored() {
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
    fn releases_owner_once() {
        let gate = QvCommitGate::new();
        let owner = gate.try_acquire().expect("gate starts free");
        let generation = owner.generation;
        drop(owner);
        assert!(!gate.release(generation));
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn admits_waiters_fifo() {
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
            await_waiters(&gate, expected + 1);
        }
        drop(tx);
        drop(held);
        let order = (0..4)
            .map(|_| rx.recv_timeout(DEADLOCK_CAP).expect("waiter completes"))
            .collect::<Vec<_>>();
        for handle in handles {
            await_thread(&handle);
            handle.join().expect("waiter thread finished");
        }
        assert_eq!(order, vec![0, 1, 2, 3]);
        assert_eq!(gate.owner_count(), 0);
    }

    #[test]
    fn release_wakes_one() {
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
        await_waiters(&gate, 4);
        drop(held);
        for handle in handles {
            await_thread(&handle);
            handle.join().expect("waiter thread finished");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 1, "owner maximum is one");
    }

    #[test]
    fn early_handoff_survives() {
        let gate = QvCommitGate::new();
        let owner = gate.try_acquire().expect("gate starts free");
        let waiter = Arc::new(Waiter {
            granted: Mutex::new(None),
            ready: Condvar::new(),
            wake_checks: AtomicUsize::new(0),
        });
        gate.lock().waiters.push_back(Arc::clone(&waiter));
        // The notification lands before the waiter ever parks.
        drop(owner);
        let granted = waiter.granted.lock().expect("uncontended slot");
        assert!(granted.is_some());
        assert_eq!(gate.owner_count(), 1);
    }

    #[test]
    fn unwind_releases_owner() {
        let gate = QvCommitGate::new();
        let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = gate.try_acquire().expect("gate starts free");
            panic!("staging failed");
        }));
        assert!(attempt.is_err());
        assert_eq!(gate.owner_count(), 0);
        assert!(gate.try_acquire().is_some(), "a healthy write proceeds");
    }

    #[test]
    fn spurious_wake_safe() {
        let gate = Arc::new(QvCommitGate::new());
        let held = gate.try_acquire().expect("gate starts free");
        let waiting = Arc::clone(&gate);
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let owner = waiting.acquire_timeout(PATIENT).expect("waiter admitted");
            tx.send(()).expect("receiver remains live");
            drop(owner);
        });
        await_waiters(&gate, 1);
        let waiter = Arc::clone(gate.lock().waiters.front().expect("waiter is queued"));
        let deadline = Instant::now() + DEADLOCK_CAP;
        while waiter.wake_checks.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "waiter did not park");
            std::thread::yield_now();
        }
        let initial = waiter.wake_checks.load(Ordering::SeqCst);
        let granted = loop {
            match waiter.granted.try_lock() {
                Ok(granted) => break granted,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    assert!(Instant::now() < deadline, "waiter did not park");
                    std::thread::yield_now();
                }
            }
        };
        waiter.ready.notify_one();
        drop(granted);
        while waiter.wake_checks.load(Ordering::SeqCst) <= initial {
            assert!(Instant::now() < deadline, "spurious wake was not observed");
            std::thread::yield_now();
        }
        assert!(rx.try_recv().is_err(), "wake cannot grant ownership");
        assert_eq!(gate.owner_count(), 1);
        drop(held);
        rx.recv_timeout(DEADLOCK_CAP)
            .expect("waiter receives handoff");
        await_thread(&handle);
        handle.join().expect("waiter thread finished");
    }

    #[test]
    fn timeout_positions() {
        // `remove_waiter` is the exact post-Condvar timeout path; direct use
        // makes queue position deterministic without wall-clock timing.
        for cancelled in 0..3 {
            let gate = QvCommitGate::new();
            let held = gate.try_acquire().expect("gate starts free");
            let waiters = (0..3).map(|_| queued_waiter()).collect::<Vec<_>>();
            gate.lock().waiters.extend(waiters.iter().cloned());
            assert!(gate.remove_waiter(&waiters[cancelled]).is_none());
            drop(held);
            for (index, waiter) in waiters.iter().enumerate() {
                let generation = *waiter.granted.lock().expect("wait slot is available");
                if index == cancelled {
                    assert!(generation.is_none(), "timed-out waiter cannot own the gate");
                } else {
                    drop(gate.owner_for(generation.expect("surviving waiter receives handoff")));
                }
            }
            assert_eq!(gate.waiting(), 0);
            assert_eq!(gate.owner_count(), 0);
        }
    }

    #[test]
    fn panic_handoff_continues() {
        let gate = Arc::new(QvCommitGate::new());
        let held = gate.try_acquire().expect("gate starts free");
        let (tx, rx) = mpsc::channel();
        let first_gate = Arc::clone(&gate);
        let first_tx = tx.clone();
        let first = std::thread::spawn(move || {
            let _owner = first_gate.acquire_timeout(PATIENT).expect("first admitted");
            first_tx.send(0).expect("receiver remains live");
            panic!("panic after handoff");
        });
        await_waiters(&gate, 1);
        let mut followers = Vec::new();
        for index in 1..=2 {
            let waiting = Arc::clone(&gate);
            let tx = tx.clone();
            followers.push(std::thread::spawn(move || {
                let owner = waiting.acquire_timeout(PATIENT).expect("follower admitted");
                tx.send(index).expect("receiver remains live");
                drop(owner);
            }));
            await_waiters(&gate, index + 1);
        }
        drop(tx);
        drop(held);
        assert_eq!(rx.recv_timeout(DEADLOCK_CAP).unwrap(), 0);
        await_thread(&first);
        assert!(first.join().is_err());
        assert_eq!(rx.recv_timeout(DEADLOCK_CAP).unwrap(), 1);
        assert_eq!(rx.recv_timeout(DEADLOCK_CAP).unwrap(), 2);
        for follower in followers {
            await_thread(&follower);
            follower.join().expect("follower thread finished");
        }
        assert_eq!(gate.owner_count(), 0);
    }
}
