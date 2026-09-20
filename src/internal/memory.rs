//! Shares an explicit process memory budget across live Craqle stores.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::path::Path;
use std::sync::{LazyLock, Mutex, PoisonError};

const MIB: u64 = 1_048_576;
const FALLBACK_PROCESS_BYTES: u64 = 4 * 1_024 * MIB;
const DEFAULT_STORAGE_BYTES: u64 = 128 * MIB;
const DEFAULT_APP_BYTES: u64 = 128 * MIB;
const DEFAULT_SEARCH_BYTES: u64 = 256 * MIB;
const DEFAULT_PREPARED_BYTES: u64 = 64 * MIB;
const MIN_COMPONENT_BYTES: u64 = 4 * MIB;
const MIN_SEARCH_BYTES: u64 = 15 * MIB;
const MIN_STORE_BYTES: u64 = 3 * MIN_COMPONENT_BYTES + MIN_SEARCH_BYTES;
const CGROUP_UNLIMITED_BYTES: u64 = 1 << 62;

#[derive(Debug, thiserror::Error)]
pub enum MemoryBudgetError {
    #[error("memory component budgets exceed the per-store budget")]
    ComponentsExceedStore,
    #[error("per-store memory budget exceeds the process budget")]
    StoreExceedsProcess,
    #[error("memory budget leaves no process or operating-system headroom")]
    MissingHeadroom,
    #[error("live stores have reserved {reserved} bytes of the {available} byte process budget")]
    ProcessExhausted { reserved: u64, available: u64 },
}

type Result<T> = std::result::Result<T, MemoryBudgetError>;

/// Configured memory reservations. These are component accounting ceilings,
/// not a promise that process RSS cannot exceed them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryBudget {
    process_bytes: u64,
    store_bytes: u64,
    storage_bytes: u64,
    application_bytes: u64,
    search_writer_bytes: u64,
    prepared_work_bytes: u64,
}

impl Default for MemoryBudget {
    fn default() -> Self {
        let process = process_memory_limit().unwrap_or(FALLBACK_PROCESS_BYTES);
        let available = process.saturating_mul(3) / 4;
        let store = available.div_ceil(32).max(MIN_STORE_BYTES);
        Self::new(process, store)
    }
}

impl MemoryBudget {
    pub fn new(process_bytes: u64, store_bytes: u64) -> Self {
        let remaining = store_bytes.saturating_sub(MIN_STORE_BYTES);
        let desired = DEFAULT_STORAGE_BYTES
            .saturating_sub(MIN_COMPONENT_BYTES)
            .saturating_add(DEFAULT_APP_BYTES.saturating_sub(MIN_COMPONENT_BYTES))
            .saturating_add(DEFAULT_SEARCH_BYTES.saturating_sub(MIN_SEARCH_BYTES))
            .saturating_add(DEFAULT_PREPARED_BYTES.saturating_sub(MIN_COMPONENT_BYTES));
        let scale = |component: u64, minimum: u64| {
            minimum.saturating_add(
                component
                    .saturating_sub(minimum)
                    .saturating_mul(remaining)
                    .checked_div(desired)
                    .unwrap_or(0),
            )
        };
        Self {
            process_bytes,
            store_bytes,
            storage_bytes: scale(DEFAULT_STORAGE_BYTES, MIN_COMPONENT_BYTES),
            application_bytes: scale(DEFAULT_APP_BYTES, MIN_COMPONENT_BYTES),
            search_writer_bytes: scale(DEFAULT_SEARCH_BYTES, MIN_SEARCH_BYTES),
            prepared_work_bytes: scale(DEFAULT_PREPARED_BYTES, MIN_COMPONENT_BYTES),
        }
    }

    pub fn with_storage(mut self, bytes: u64) -> Self {
        self.storage_bytes = bytes;
        self
    }

    pub fn with_application(mut self, bytes: u64) -> Self {
        self.application_bytes = bytes;
        self
    }

    pub fn with_search_writer(mut self, bytes: u64) -> Self {
        self.search_writer_bytes = bytes;
        self
    }

    pub fn with_prepared_work(mut self, bytes: u64) -> Self {
        self.prepared_work_bytes = bytes;
        self
    }

    pub fn process_bytes(self) -> u64 {
        self.process_bytes
    }

    pub fn store_bytes(self) -> u64 {
        self.store_bytes
    }

    pub fn storage_bytes(self) -> u64 {
        self.storage_bytes
    }

    pub fn application_bytes(self) -> u64 {
        self.application_bytes
    }

    pub fn search_writer_bytes(self) -> u64 {
        self.search_writer_bytes
    }

    pub fn prepared_work_bytes(self) -> u64 {
        self.prepared_work_bytes
    }

    pub(crate) fn reserve(self) -> Result<MemoryLease> {
        self.validate()?;
        let mut state = PROCESS_BUDGET
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.reserve(self.process_bytes, self.store_bytes)?;
        Ok(MemoryLease {
            bytes: self.store_bytes,
        })
    }

    pub fn validate(self) -> std::result::Result<(), MemoryBudgetError> {
        let components = self
            .storage_bytes
            .saturating_add(self.application_bytes)
            .saturating_add(self.search_writer_bytes)
            .saturating_add(self.prepared_work_bytes);
        if components > self.store_bytes {
            return Err(MemoryBudgetError::ComponentsExceedStore);
        }
        if self.store_bytes > self.process_bytes {
            return Err(MemoryBudgetError::StoreExceedsProcess);
        }
        if self.store_bytes > self.process_bytes.saturating_mul(3) / 4 {
            return Err(MemoryBudgetError::MissingHeadroom);
        }
        Ok(())
    }
}

struct ProcessBudget {
    process_bytes: Option<u64>,
    reserved_bytes: u64,
}

impl ProcessBudget {
    fn new() -> Self {
        Self {
            process_bytes: None,
            reserved_bytes: 0,
        }
    }

    fn reserve(&mut self, process: u64, store: u64) -> Result<()> {
        // Defaults observe volatile MemAvailable values. Keep the smallest
        // observation while leases live, and never expand admission from a later rise.
        let process = self
            .process_bytes
            .map_or(process, |current| current.min(process));
        self.process_bytes = Some(process);
        let available = process.saturating_mul(3) / 4;
        let reserved = self.reserved_bytes.saturating_add(store);
        if reserved > available {
            return Err(MemoryBudgetError::ProcessExhausted {
                reserved,
                available,
            });
        }
        self.reserved_bytes = reserved;
        Ok(())
    }

    fn release(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
        if self.reserved_bytes == 0 {
            self.process_bytes = None;
        }
    }
}

static PROCESS_BUDGET: LazyLock<Mutex<ProcessBudget>> =
    LazyLock::new(|| Mutex::new(ProcessBudget::new()));

pub(crate) struct MemoryLease {
    bytes: u64,
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        let mut state = PROCESS_BUDGET
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.release(self.bytes);
    }
}

fn process_memory_limit() -> Option<u64> {
    let host = meminfo_available(Path::new("/proc/meminfo"));
    let cgroup = cgroup_limit(Path::new("/proc/self/cgroup"), Path::new("/sys/fs/cgroup"));
    match (host, cgroup) {
        (Some(host), Some(cgroup)) => Some(host.min(cgroup)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn meminfo_available(path: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines().find_map(|line| {
        let kib = line
            .strip_prefix("MemAvailable:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        kib.checked_mul(1024)
    })
}

fn cgroup_limit(mapping: &Path, root: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(mapping).ok()?;
    let mut limit = None;
    for line in text.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let found = if hierarchy == "0" && controllers.is_empty() {
            smallest_limit(root, Path::new(path), "memory.max")
        } else if controllers.split(',').any(|name| name == "memory") {
            smallest_limit(
                &root.join("memory"),
                Path::new(path),
                "memory.limit_in_bytes",
            )
        } else {
            continue;
        };
        if let Some(value) = found {
            limit = Some(limit.map_or(value, |current: u64| current.min(value)));
        }
    }
    limit
}

fn smallest_limit(root: &Path, relative: &Path, file: &str) -> Option<u64> {
    let mut limit = numeric_limit(&root.join(file));
    let mut directory = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        directory.push(name);
        if let Some(value) = numeric_limit(&directory.join(file)) {
            limit = Some(limit.map_or(value, |current: u64| current.min(value)));
        }
    }
    limit
}

fn numeric_limit(path: &Path) -> Option<u64> {
    let value = std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    (value < CGROUP_UNLIMITED_BYTES).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_budget_scales() {
        let budget = MemoryBudget::new(256 * MIB, 128 * MIB)
            .with_storage(32 * MIB)
            .with_application(32 * MIB)
            .with_search_writer(32 * MIB)
            .with_prepared_work(16 * MIB);
        budget.validate().unwrap();
        let mut pool = ProcessBudget::new();
        assert!(
            pool.reserve(budget.process_bytes(), budget.store_bytes())
                .is_ok()
        );
        assert!(budget.storage_bytes() < 1_024 * MIB);
    }

    #[test]
    fn stores_share_budget() {
        let budget = MemoryBudget::new(512 * MIB, 192 * MIB)
            .with_storage(32 * MIB)
            .with_application(32 * MIB)
            .with_search_writer(64 * MIB)
            .with_prepared_work(16 * MIB);
        budget.validate().unwrap();
        let mut pool = ProcessBudget::new();
        pool.reserve(budget.process_bytes(), budget.store_bytes())
            .unwrap();
        pool.reserve(budget.process_bytes(), budget.store_bytes())
            .unwrap();
        assert!(
            pool.reserve(budget.process_bytes(), budget.store_bytes())
                .is_err()
        );
        pool.release(budget.store_bytes());
        assert!(
            pool.reserve(budget.process_bytes(), budget.store_bytes())
                .is_ok()
        );
    }

    #[test]
    fn observations_only_lower() {
        let mut pool = ProcessBudget::new();
        pool.reserve(1_024 * MIB, 64 * MIB).unwrap();
        pool.reserve(1_000 * MIB, 64 * MIB).unwrap();
        pool.reserve(2_048 * MIB, 64 * MIB).unwrap();
        assert_eq!(pool.process_bytes, Some(1_000 * MIB));

        assert!(pool.reserve(200 * MIB, 1).is_err());
        assert_eq!(pool.process_bytes, Some(200 * MIB));
        pool.release(64 * MIB);
        assert!(pool.reserve(2_048 * MIB, 32 * MIB).is_err());
        pool.release(128 * MIB);
        assert_eq!(pool.process_bytes, None);
        assert!(pool.reserve(2_048 * MIB, 64 * MIB).is_ok());
    }
}
