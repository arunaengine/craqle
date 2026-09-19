//! Bounds cached entries and maintains their recency order.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CacheStatistics {
    pub(crate) entries: usize,
    pub(crate) bytes: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) evictions: u64,
    /// Keys the removal predicate was asked about.
    pub(crate) inspections: u64,
    /// Times the recency order was rebuilt and sorted.
    pub(crate) compactions: u64,
}

struct CacheEntry<V> {
    value: V,
    bytes: usize,
    stamp: u64,
}

pub(crate) struct BoundedCache<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    order: VecDeque<(u64, K)>,
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
    stamp: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    #[cfg(test)]
    inspections: u64,
    #[cfg(test)]
    compactions: u64,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_entries,
            max_bytes,
            bytes: 0,
            stamp: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            #[cfg(test)]
            inspections: 0,
            #[cfg(test)]
            compactions: 0,
        }
    }

    pub(crate) fn get_cloned(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        self.hits = self.hits.saturating_add(1);
        self.stamp = self.stamp.wrapping_add(1);
        entry.stamp = self.stamp;
        self.order.push_back((self.stamp, key.clone()));
        let value = entry.value.clone();
        self.compact_order_if_needed();
        Some(value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V, bytes: usize) {
        if self.max_entries == 0 || bytes > self.max_bytes {
            self.remove(&key);
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(previous.bytes);
        }
        self.stamp = self.stamp.wrapping_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(
            key.clone(),
            CacheEntry {
                value,
                bytes,
                stamp: self.stamp,
            },
        );
        self.order.push_back((self.stamp, key));
        while self.entries.len() > self.max_entries || self.bytes > self.max_bytes {
            self.evict_oldest();
        }
        self.compact_order_if_needed();
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.bytes = self.bytes.saturating_sub(entry.bytes);
        Some(entry.value)
    }

    pub(crate) fn remove_where(&mut self, mut predicate: impl FnMut(&K) -> bool) {
        #[cfg(test)]
        {
            self.inspections = self.inspections.saturating_add(self.entries.len() as u64);
        }
        let keys = self
            .entries
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return;
        }
        for key in keys {
            self.remove(&key);
        }
        self.compact_order();
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> CacheStatistics {
        CacheStatistics {
            entries: self.entries.len(),
            bytes: self.bytes,
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            inspections: self.inspections,
            compactions: self.compactions,
        }
    }

    fn evict_oldest(&mut self) {
        while let Some((stamp, key)) = self.order.pop_front() {
            if self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.stamp == stamp)
            {
                self.remove(&key);
                self.evictions = self.evictions.saturating_add(1);
                return;
            }
        }
    }

    fn compact_order_if_needed(&mut self) {
        let maximum = self.entries.len().saturating_mul(4).max(64);
        if self.order.len() > maximum {
            self.compact_order();
        }
    }

    /// Drops superseded recency records. Stamps only increase and records are
    /// only appended, so the deque is already ascending and one pass is enough;
    /// rebuilding it from the unordered map forced a sort that changed nothing.
    fn compact_order(&mut self) {
        #[cfg(test)]
        {
            self.compactions = self.compactions.saturating_add(1);
        }
        let entries = &self.entries;
        self.order
            .retain(|(stamp, key)| entries.get(key).is_some_and(|entry| entry.stamp == *stamp));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_preserves_recency() {
        let mut cache: BoundedCache<u64, u64> = BoundedCache::new(8, 4_096);
        for key in 0..8u64 {
            cache.insert(key, key, 8);
        }
        // Touch in a deliberately different order than insertion.
        for key in [3u64, 0, 7, 1] {
            assert_eq!(cache.get_cloned(&key), Some(key));
        }
        // Force compaction, then check the oldest entry is evicted first.
        cache.compact_order();
        assert_eq!(cache.order.len(), cache.entries.len());
        let stamps: Vec<u64> = cache.order.iter().map(|(stamp, _)| *stamp).collect();
        let mut sorted = stamps.clone();
        sorted.sort_unstable();
        assert_eq!(stamps, sorted, "compaction must keep ascending recency");
        cache.insert(100, 100, 8);
        assert_eq!(
            cache.get_cloned(&2),
            None,
            "the least recent entry is evicted"
        );
        assert_eq!(cache.get_cloned(&3), Some(3));
    }

    #[test]
    fn bounded_cache_eviction() {
        let mut cache = BoundedCache::new(2, 8);
        cache.insert(1, "one", 3);
        cache.insert(2, "two", 3);
        assert_eq!(Some("one"), cache.get_cloned(&1));
        cache.insert(3, "three", 3);

        assert_eq!(Some("one"), cache.get_cloned(&1));
        assert_eq!(None, cache.get_cloned(&2));
        assert_eq!(Some("three"), cache.get_cloned(&3));
        assert_eq!(2, cache.statistics().entries);
        assert_eq!(1, cache.statistics().evictions);
    }
}
