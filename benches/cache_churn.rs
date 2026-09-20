//! Compares production cache behavior with an exact-recency control.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, VecDeque};
use std::hint::black_box;
use std::mem::size_of;
use std::time::Instant;

use serde_json::{Value, json};

// The standalone harness imports production helpers and their test module.
#[allow(dead_code, unused_imports)]
#[path = "../src/internal/cache.rs"]
mod cache;

use cache::BoundedCache;

fn main() {
    let capacity = env_usize("CRAQLE_CACHE_ENTRIES", 4_096).max(1);
    let samples = env_usize("CRAQLE_CACHE_SAMPLES", 100_000).max(capacity);
    let locality = locality_trace(samples, capacity);
    let churn = churn_trace(samples, capacity);
    println!(
        "{}",
        json!({
            "benchmark_id": "B15",
            "status": "measured",
            "case": {"capacity": capacity, "samples": samples},
            "result": {
                "completed": samples * 2 + capacity,
                "traces": [
                    report("locality", &locality, capacity),
                    report("churn", &churn, capacity),
                    invalidation_report(capacity),
                ]
            }
        })
    );
}

#[derive(Default)]
struct Counts {
    hits: u64,
    misses: u64,
    evictions: u64,
    checksum: u64,
}

struct ExactCache {
    entries: HashMap<u64, u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl ExactCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&mut self, key: u64) -> Option<u64> {
        let value = self.entries.get(&key).copied()?;
        self.order.retain(|queued| *queued != key);
        self.order.push_back(key);
        Some(value)
    }

    fn insert(&mut self, key: u64, value: u64) -> bool {
        if self.entries.insert(key, value).is_some() {
            self.order.retain(|queued| *queued != key);
            self.order.push_back(key);
            return false;
        }
        self.order.push_back(key);
        if self.entries.len() <= self.capacity {
            return false;
        }
        let oldest = self.order.pop_front().unwrap();
        self.entries.remove(&oldest);
        true
    }

    fn remove_where(&mut self, predicate: impl Fn(u64) -> bool) -> usize {
        let before = self.entries.len();
        self.entries.retain(|key, _| !predicate(*key));
        self.order.retain(|key| self.entries.contains_key(key));
        before - self.entries.len()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn locality_trace(samples: usize, capacity: usize) -> Vec<u64> {
    let hot = usize::max(1, capacity / 8);
    (0..samples)
        .map(|index| {
            if index % 10 < 9 {
                (index % hot) as u64
            } else {
                (capacity + index) as u64
            }
        })
        .collect()
}

fn churn_trace(samples: usize, capacity: usize) -> Vec<u64> {
    let window = capacity.saturating_mul(4).max(1);
    (0..samples)
        .map(|index| ((index.wrapping_mul(2_654_435_761usize)) % window) as u64)
        .collect()
}

fn run_exact(trace: &[u64], capacity: usize) -> Counts {
    let mut cache = ExactCache::new(capacity);
    let mut counts = Counts::default();
    for key in trace {
        let value = match cache.get(*key) {
            Some(value) => {
                counts.hits += 1;
                value
            }
            None => {
                counts.misses += 1;
                let value = key.wrapping_mul(17);
                counts.evictions += u64::from(cache.insert(*key, value));
                value
            }
        };
        counts.checksum = counts.checksum.wrapping_add(black_box(value));
    }
    counts
}

fn run_actual(trace: &[u64], capacity: usize) -> (Counts, u128) {
    let entry_bytes = size_of::<u64>();
    let byte_cap = capacity.saturating_mul(256).max(1_024);
    let mut cache = BoundedCache::new(capacity, byte_cap);
    let mut counts = Counts::default();
    let started = Instant::now();
    for key in trace {
        let value = match cache.get_cloned(key) {
            Some(value) => {
                counts.hits += 1;
                value
            }
            None => {
                counts.misses += 1;
                counts.evictions += u64::from(counts.misses as usize > capacity);
                let value = key.wrapping_mul(17);
                cache.insert(*key, value, entry_bytes);
                value
            }
        };
        counts.checksum = counts.checksum.wrapping_add(black_box(value));
    }
    (counts, started.elapsed().as_nanos())
}

fn report(trace: &str, keys: &[u64], capacity: usize) -> Value {
    let exact = run_exact(keys, capacity);
    let (actual, actual_ns) = run_actual(keys, capacity);
    assert_eq!(
        exact.checksum, actual.checksum,
        "cache policies changed source results"
    );
    let exact_bytes = capacity.saturating_mul(size_of::<(u64, u64)>() * 2 + size_of::<u64>());
    let actual_bytes = capacity.saturating_mul(size_of::<(u64, u64)>() * 4 + 40);
    json!({
        "trace": trace,
        "samples": keys.len(),
        "exact_retained_bytes_estimate": exact_bytes,
        "actual_retained_bytes_estimate": actual_bytes,
        "exact": {
            "hits": exact.hits,
            "misses": exact.misses,
            "evictions": exact.evictions,
            "checksum": exact.checksum,
            "timing": "semantic-control-only",
        },
        "actual": {
            "hits": actual.hits,
            "misses": actual.misses,
            "evictions_estimate": actual.evictions,
            "checksum": actual.checksum,
            "work_ns": actual_ns,
        }
    })
}

fn invalidation_report(capacity: usize) -> Value {
    let mut exact = ExactCache::new(capacity);
    let mut actual = BoundedCache::new(capacity, capacity.saturating_mul(256).max(1_024));
    for key in 0..capacity as u64 {
        exact.insert(key, key.wrapping_mul(17));
        actual.insert(key, key.wrapping_mul(17), size_of::<u64>());
    }
    let exact_invalidated = exact.remove_where(|key| key % 4 == 0);
    let expected_invalidated = (0..capacity as u64).filter(|key| key % 4 == 0).count();
    let started = Instant::now();
    actual.remove_where(|key| *key % 4 == 0);
    let invalidation_ns = started.elapsed().as_nanos();
    let mut exact_checksum = 0u64;
    let mut actual_checksum = 0u64;
    let mut actual_hits = 0u64;
    let mut actual_misses = 0u64;
    for key in 0..capacity as u64 {
        let exact_value = exact.get(key).unwrap_or_else(|| {
            let value = key.wrapping_mul(17);
            exact.insert(key, value);
            value
        });
        let actual_value = match actual.get_cloned(&key) {
            Some(value) => {
                actual_hits += 1;
                value
            }
            None => {
                actual_misses += 1;
                let value = key.wrapping_mul(17);
                actual.insert(key, value, size_of::<u64>());
                value
            }
        };
        exact_checksum = exact_checksum.wrapping_add(exact_value);
        actual_checksum = actual_checksum.wrapping_add(actual_value);
    }
    assert_eq!(exact_invalidated, expected_invalidated);
    assert_eq!(
        exact_checksum, actual_checksum,
        "invalidation changed source results"
    );
    json!({
        "trace": "fixed-invalidation",
        "samples": capacity,
        "exact_retained_bytes_estimate": capacity.saturating_mul(size_of::<(u64, u64)>() * 2 + size_of::<u64>()),
        "actual_retained_bytes_estimate": capacity.saturating_mul(size_of::<(u64, u64)>() * 4 + 40),
        "exact": {
            "invalidated": exact_invalidated,
            "checksum": exact_checksum,
            "timing": "semantic-control-only",
        },
        "actual": {
            "invalidated_expected": expected_invalidated,
            "hits": actual_hits,
            "misses": actual_misses,
            "checksum": actual_checksum,
            "invalidation_ns": invalidation_ns,
            "work_ns": invalidation_ns,
        }
    })
}
