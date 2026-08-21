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
        let keys = self
            .entries
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect::<Vec<_>>();
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

    fn compact_order(&mut self) {
        let mut order = self
            .entries
            .iter()
            .map(|(key, entry)| (entry.stamp, key.clone()))
            .collect::<Vec<_>>();
        order.sort_unstable_by_key(|(stamp, _)| *stamp);
        self.order = order.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
