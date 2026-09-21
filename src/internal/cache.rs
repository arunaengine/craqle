//! Bounds cached entries and maintains their recency order.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::mem::size_of;

/// Conservative allocator, control-byte, and indirect handle reserve per entry.
const ENTRY_RESERVE: usize = 4 * size_of::<usize>();
const ALLOCATION_RESERVE: usize = 2 * size_of::<usize>();

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
    payload_bytes: usize,
}

pub(crate) struct BoundedCache<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    order: VecDeque<K>,
    max_entries: usize,
    max_bytes: usize,
    payload_bytes: usize,
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
            payload_bytes: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            #[cfg(test)]
            inspections: 0,
            #[cfg(test)]
            compactions: 0,
        }
    }

    pub(crate) fn get_cloned<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let Some(entry) = self.entries.get(key) else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        self.hits = self.hits.saturating_add(1);
        Some(entry.value.clone())
    }

    /// Reads under a shared borrow. The caller counts the hit, because this
    /// path never reorders entries and takes no exclusive lock.
    pub(crate) fn peek<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.entries.get(key).map(|entry| &entry.value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V, bytes: usize) {
        if self.max_entries == 0 || bytes.saturating_add(Self::entry_reserve()) > self.max_bytes {
            self.remove(&key);
            return;
        }
        if let Some(previous) = self.entries.get_mut(&key) {
            self.payload_bytes = self.payload_bytes.saturating_sub(previous.payload_bytes);
            self.payload_bytes = self.payload_bytes.saturating_add(bytes);
            previous.value = value;
            previous.payload_bytes = bytes;
            self.enforce_limits();
            return;
        }
        self.payload_bytes = self.payload_bytes.saturating_add(bytes);
        self.entries.insert(
            key.clone(),
            CacheEntry {
                value,
                payload_bytes: bytes,
            },
        );
        self.order.push_back(key);
        self.enforce_limits();
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.payload_bytes = self.payload_bytes.saturating_sub(entry.payload_bytes);
        self.order.retain(|queued| queued != key);
        if self.entries.is_empty() || self.retained_bytes() > self.max_bytes {
            self.shrink_storage();
        }
        Some(entry.value)
    }

    pub(crate) fn remove_where(&mut self, mut predicate: impl FnMut(&K) -> bool) {
        #[cfg(test)]
        {
            self.inspections = self.inspections.saturating_add(self.entries.len() as u64);
        }
        let before = self.entries.len();
        let payload_bytes = &mut self.payload_bytes;
        self.entries.retain(|key, entry| {
            if predicate(key) {
                *payload_bytes = payload_bytes.saturating_sub(entry.payload_bytes);
                false
            } else {
                true
            }
        });
        if self.entries.len() == before {
            return;
        }
        #[cfg(test)]
        {
            self.compactions = self.compactions.saturating_add(1);
        }
        let entries = &self.entries;
        self.order.retain(|key| entries.contains_key(key));
        self.shrink_storage();
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.entries.shrink_to_fit();
        self.order.shrink_to_fit();
        self.payload_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> CacheStatistics {
        CacheStatistics {
            entries: self.entries.len(),
            bytes: self.retained_bytes(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            inspections: self.inspections,
            compactions: self.compactions,
        }
    }

    fn evict_oldest(&mut self) {
        if let Some(key) = self.order.pop_front()
            && let Some(entry) = self.entries.remove(&key)
        {
            self.payload_bytes = self.payload_bytes.saturating_sub(entry.payload_bytes);
            self.evictions = self.evictions.saturating_add(1);
        }
    }

    fn enforce_limits(&mut self) {
        while self.entries.len() > self.max_entries || self.estimated_bytes() > self.max_bytes {
            self.evict_oldest();
        }
        if self.retained_bytes() > self.max_bytes {
            self.shrink_storage();
        }
        while self.retained_bytes() > self.max_bytes && !self.entries.is_empty() {
            self.evict_oldest();
            self.shrink_storage();
        }
    }

    fn entry_reserve() -> usize {
        size_of::<K>()
            .saturating_add(size_of::<CacheEntry<V>>())
            .saturating_add(size_of::<K>())
            .saturating_add(ENTRY_RESERVE)
    }

    /// Reserves two hash buckets per live entry for load factor and growth.
    fn estimated_bytes(&self) -> usize {
        if self.entries.is_empty() {
            return 0;
        }
        self.payload_bytes
            .saturating_add(
                self.entries
                    .len()
                    .saturating_mul(Self::entry_reserve().saturating_mul(2)),
            )
            .saturating_add(ALLOCATION_RESERVE)
    }

    fn retained_bytes(&self) -> usize {
        if self.entries.is_empty() && self.entries.capacity() == 0 && self.order.capacity() == 0 {
            return 0;
        }
        let bucket_bytes = size_of::<(K, CacheEntry<V>)>().saturating_add(size_of::<usize>());
        self.payload_bytes
            .saturating_add(
                self.entries
                    .capacity()
                    .saturating_mul(2)
                    .saturating_mul(bucket_bytes),
            )
            .saturating_add(self.order.capacity().saturating_mul(size_of::<K>()))
            .saturating_add(self.entries.len().saturating_mul(ENTRY_RESERVE))
            .saturating_add(ALLOCATION_RESERVE)
    }

    fn shrink_storage(&mut self) {
        self.entries.shrink_to_fit();
        self.order.shrink_to_fit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hits_bound_work() {
        let mut cache: BoundedCache<u64, u64> = BoundedCache::new(8, 4_096);
        for key in 0..8u64 {
            cache.insert(key, key, 8);
        }
        for _ in 0..1_000 {
            assert_eq!(cache.get_cloned(&3), Some(3));
        }
        assert_eq!(cache.order.len(), cache.entries.len());
        assert_eq!(cache.statistics().compactions, 0);
        cache.insert(100, 100, 8);
        assert_eq!(cache.get_cloned(&0), None);
        assert_eq!(cache.get_cloned(&3), Some(3));
    }

    #[test]
    fn bounded_cache_eviction() {
        let mut cache = BoundedCache::new(2, 1_024);
        cache.insert(1, "one", 3);
        cache.insert(2, "two", 3);
        assert_eq!(Some("one"), cache.get_cloned(&1));
        cache.insert(3, "three", 3);

        assert_eq!(None, cache.get_cloned(&1));
        assert_eq!(Some("two"), cache.get_cloned(&2));
        assert_eq!(Some("three"), cache.get_cloned(&3));
        assert_eq!(2, cache.statistics().entries);
        assert_eq!(1, cache.statistics().evictions);
    }

    #[test]
    fn bytes_include_storage() {
        let mut cache = BoundedCache::new(8, 1_024);
        cache.insert(1u64, 2u64, 8);
        assert!(cache.statistics().bytes > 8);
        assert!(cache.statistics().bytes <= 1_024);
    }

    #[test]
    fn oversized_replace_releases() {
        let mut cache = BoundedCache::new(1_024, 64 * 1_024);
        for key in 0..256u64 {
            cache.insert(key, key, 8);
        }
        cache.insert(0, 0, usize::MAX);
        for key in 1..256u64 {
            cache.remove(&key);
        }
        assert_eq!(cache.statistics().entries, 0);
        assert_eq!(cache.statistics().bytes, 0);
    }
}
