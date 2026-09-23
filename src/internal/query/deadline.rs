//! Cancels active query evaluators when request deadlines expire.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::context::{CancelRegistration, QueryCancellation, RequestOutcome, RequestState};

#[derive(Clone)]
pub(crate) struct RequestClock {
    inner: Arc<RequestClockInner>,
}

struct RequestClockInner {
    cancellation: QueryCancellation,
    evaluator: CancelRegistration,
    _deadline: DeadlineRegistration,
}

impl RequestClock {
    pub(crate) fn start(
        timeout: Option<Duration>,
        cancellation: QueryCancellation,
        started: Instant,
    ) -> Self {
        let evaluator = cancellation.register();
        let expires = timeout.and_then(|timeout| started.checked_add(timeout));
        if timeout.is_some() && expires.is_none() {
            evaluator.state().mark_deadline();
        }
        let deadline = DeadlineRegistration::new(expires, &evaluator);
        Self {
            inner: Arc::new(RequestClockInner {
                cancellation,
                evaluator,
                _deadline: deadline,
            }),
        }
    }

    pub(crate) fn evaluator(&self) -> spareval::CancellationToken {
        self.inner.evaluator.evaluator()
    }

    pub(crate) fn outcome(&self) -> RequestOutcome {
        let outcome = self.inner.evaluator.outcome();
        if self.inner.cancellation.is_cancelled() {
            RequestOutcome::Explicit
        } else {
            outcome
        }
    }
}

struct DeadlineRegistration {
    id: Option<u128>,
    state: Arc<RequestState>,
}

impl DeadlineRegistration {
    fn new(expires: Option<Instant>, evaluator: &CancelRegistration) -> Self {
        let state = evaluator.state();
        let id = if evaluator.outcome() == RequestOutcome::Active {
            expires.and_then(|expires| deadline_service().register(expires, &state))
        } else {
            None
        };
        Self { id, state }
    }
}

impl Drop for DeadlineRegistration {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            deadline_service().remove(id, &self.state);
        }
    }
}

struct DeadlineService {
    shared: Arc<DeadlineShared>,
    available: bool,
}

struct DeadlineShared {
    schedule: Mutex<DeadlineSchedule>,
    changed: Condvar,
}

#[derive(Default)]
struct DeadlineSchedule {
    next_id: u128,
    deadlines: BTreeMap<Instant, HashMap<u128, Arc<RequestState>>>,
    index: HashMap<u128, (Instant, Arc<RequestState>)>,
}

struct DeadlineEntry {
    expires: Instant,
    state: Arc<RequestState>,
}

pub(crate) const MAX_DEADLINE_REGISTRATIONS: usize = 65_536;

impl DeadlineService {
    fn new() -> Self {
        let shared = Arc::new(DeadlineShared {
            schedule: Mutex::new(DeadlineSchedule::default()),
            changed: Condvar::new(),
        });
        let worker = Arc::clone(&shared);
        let available = std::thread::Builder::new()
            .name("craqle-query-deadlines".to_owned())
            .spawn(move || run_deadlines(&worker))
            .is_ok();
        Self { shared, available }
    }

    fn register(&self, expires: Instant, state: &Arc<RequestState>) -> Option<u128> {
        if expires <= Instant::now() {
            state.mark_deadline();
            return None;
        }
        if !self.available {
            state.mark_unavailable();
            return None;
        }
        let mut schedule = self
            .shared
            .schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = schedule.admit(
            DeadlineEntry {
                expires,
                state: Arc::clone(state),
            },
            MAX_DEADLINE_REGISTRATIONS,
        );
        if id.is_none() {
            drop(schedule);
            state.mark_capacity();
            return None;
        }
        drop(schedule);
        self.shared.changed.notify_one();
        id
    }

    fn remove(&self, id: u128, state: &Arc<RequestState>) {
        let mut schedule = self
            .shared
            .schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !schedule.remove(id, state) {
            return;
        }
        drop(schedule);
        self.shared.changed.notify_one();
    }
}

impl DeadlineSchedule {
    fn admit(&mut self, entry: DeadlineEntry, limit: usize) -> Option<u128> {
        if self.index.len() >= limit {
            return None;
        }
        let next_id = self.next_id.checked_add(1)?;
        let id = self.next_id;
        self.next_id = next_id;
        self.index
            .insert(id, (entry.expires, Arc::clone(&entry.state)));
        self.deadlines
            .entry(entry.expires)
            .or_default()
            .insert(id, entry.state);
        Some(id)
    }

    fn remove(&mut self, id: u128, state: &Arc<RequestState>) -> bool {
        let Some((expires, registered)) = self.index.get(&id) else {
            return false;
        };
        if !Arc::ptr_eq(registered, state) {
            return false;
        }
        let expires = *expires;
        self.index.remove(&id);
        if let Some(entries) = self.deadlines.get_mut(&expires) {
            entries.remove(&id);
            if entries.is_empty() {
                self.deadlines.remove(&expires);
            }
        }
        true
    }
}

fn deadline_service() -> &'static DeadlineService {
    static SERVICE: OnceLock<DeadlineService> = OnceLock::new();
    SERVICE.get_or_init(DeadlineService::new)
}

fn run_deadlines(shared: &DeadlineShared) {
    let mut schedule = shared
        .schedule
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let Some(expires) = schedule.deadlines.first_key_value().map(|(time, _)| *time) else {
            schedule = shared
                .changed
                .wait(schedule)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continue;
        };
        let now = Instant::now();
        if expires > now {
            let waited = shared.changed.wait_timeout(schedule, expires - now);
            schedule = waited.unwrap_or_else(std::sync::PoisonError::into_inner).0;
            continue;
        }
        let Some(due) = schedule.deadlines.remove(&expires) else {
            continue;
        };
        let mut expired = Vec::with_capacity(due.len());
        for (id, state) in due {
            if schedule
                .index
                .get(&id)
                .is_some_and(|(_, registered)| Arc::ptr_eq(registered, &state))
            {
                schedule.index.remove(&id);
                expired.push(state);
            }
        }
        drop(schedule);
        for state in expired {
            state.mark_deadline();
        }
        schedule = shared
            .schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_bounds_entries() {
        let mut schedule = DeadlineSchedule::default();
        let expires = Instant::now() + Duration::from_secs(60);
        let first = Arc::new(RequestState::new());
        let second = Arc::new(RequestState::new());
        let id = schedule
            .admit(
                DeadlineEntry {
                    expires,
                    state: Arc::clone(&first),
                },
                1,
            )
            .expect("entry must be admitted");
        assert!(
            schedule
                .admit(
                    DeadlineEntry {
                        expires,
                        state: second,
                    },
                    1,
                )
                .is_none()
        );
        assert!(schedule.remove(id, &first));
        assert!(
            schedule
                .admit(
                    DeadlineEntry {
                        expires,
                        state: Arc::new(RequestState::new()),
                    },
                    1,
                )
                .is_some()
        );

        let mut exhausted = DeadlineSchedule {
            next_id: u128::MAX,
            ..DeadlineSchedule::default()
        };
        assert!(
            exhausted
                .admit(
                    DeadlineEntry {
                        expires,
                        state: Arc::new(RequestState::new()),
                    },
                    1,
                )
                .is_none()
        );
    }

    #[test]
    fn stale_remove_preserves() {
        let mut schedule = DeadlineSchedule::default();
        let expires = Instant::now() + Duration::from_secs(60);
        let original = Arc::new(RequestState::new());
        let id = schedule
            .admit(
                DeadlineEntry {
                    expires,
                    state: Arc::clone(&original),
                },
                1,
            )
            .expect("entry must be admitted");
        let replacement = Arc::new(RequestState::new());
        schedule
            .index
            .insert(id, (expires, Arc::clone(&replacement)));
        schedule
            .deadlines
            .entry(expires)
            .or_default()
            .insert(id, Arc::clone(&replacement));
        assert!(!schedule.remove(id, &original));
        assert!(
            schedule
                .index
                .get(&id)
                .is_some_and(|(_, state)| Arc::ptr_eq(state, &replacement))
        );
    }

    #[test]
    fn unavailable_fails_closed() {
        let service = DeadlineService {
            shared: Arc::new(DeadlineShared {
                schedule: Mutex::new(DeadlineSchedule::default()),
                changed: Condvar::new(),
            }),
            available: false,
        };
        let state = Arc::new(RequestState::new());
        assert!(
            service
                .register(Instant::now() + Duration::from_secs(60), &state)
                .is_none()
        );
        assert_eq!(state.outcome(), RequestOutcome::DeadlineUnavailable);
    }

    #[test]
    fn capacity_fails_closed() {
        let service = DeadlineService {
            shared: Arc::new(DeadlineShared {
                schedule: Mutex::new(DeadlineSchedule {
                    next_id: u128::MAX,
                    ..DeadlineSchedule::default()
                }),
                changed: Condvar::new(),
            }),
            available: true,
        };
        let state = Arc::new(RequestState::new());
        assert!(
            service
                .register(Instant::now() + Duration::from_secs(60), &state)
                .is_none()
        );
        assert_eq!(state.outcome(), RequestOutcome::DeadlineCapacity);
    }
}
