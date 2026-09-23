//! Accounts bounded work and retained query results.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::EncodedTerm;
use crate::sparql::QueryLimits;

use super::context::{MAX_QUERY_REGISTRATIONS, RequestOutcome};
use super::deadline::{MAX_DEADLINE_REGISTRATIONS, RequestClock};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BudgetShape {
    pub(crate) property_path: bool,
    pub(crate) property_path_depth: usize,
    pub(crate) guarded_hash: bool,
    pub(crate) estimated_rows: usize,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum QueryLimitExceeded {
    #[error("{resource} exceeds limit {limit}")]
    Limit {
        resource: &'static str,
        limit: usize,
    },
    #[error("query cancelled")]
    Cancelled,
}

impl QueryLimitExceeded {
    fn limit(resource: &'static str, limit: usize) -> Self {
        Self::Limit { resource, limit }
    }
}

const HASH_ROW_ESTIMATE: usize = 128;
const DENSE_CACHE_ENTRIES: usize = 8_192;
const DENSE_CACHE_BYTES: usize = 1_048_576;

struct BudgetLimit {
    value: usize,
    resource: &'static str,
}

pub(crate) struct QueryBudget {
    limits: QueryLimits,
    clock: RequestClock,
    shape: BudgetShape,
    intermediate_rows: AtomicUsize,
    property_path_edges: AtomicUsize,
    result_rows: AtomicUsize,
    result_cells: AtomicUsize,
    result_bytes: AtomicUsize,
    graph_triples: AtomicUsize,
}

impl QueryBudget {
    pub(crate) fn new(
        shape: BudgetShape,
        limits: QueryLimits,
        clock: RequestClock,
    ) -> std::result::Result<Self, QueryLimitExceeded> {
        if let Some(error) = clock_error(&clock) {
            return Err(error);
        }
        if shape.property_path_depth > limits.max_property_path_depth {
            return Err(QueryLimitExceeded::limit(
                "property path depth",
                limits.max_property_path_depth,
            ));
        }
        if shape.estimated_rows > limits.max_intermediate_rows {
            return Err(QueryLimitExceeded::limit(
                "estimated intermediate rows",
                limits.max_intermediate_rows,
            ));
        }
        if shape.guarded_hash && shape.estimated_rows > limits.max_hash_entries {
            return Err(QueryLimitExceeded::limit(
                "estimated hash entries",
                limits.max_hash_entries,
            ));
        }
        if shape.guarded_hash
            && shape.estimated_rows.saturating_mul(HASH_ROW_ESTIMATE) > limits.max_hash_bytes
        {
            return Err(QueryLimitExceeded::limit(
                "estimated hash bytes",
                limits.max_hash_bytes,
            ));
        }
        Ok(Self {
            limits,
            clock,
            shape,
            intermediate_rows: AtomicUsize::new(0),
            property_path_edges: AtomicUsize::new(0),
            result_rows: AtomicUsize::new(0),
            result_cells: AtomicUsize::new(0),
            result_bytes: AtomicUsize::new(0),
            graph_triples: AtomicUsize::new(0),
        })
    }

    /// Applies the final query shape while keeping work already charged.
    pub(crate) fn resume(
        &self,
        shape: BudgetShape,
    ) -> std::result::Result<Self, QueryLimitExceeded> {
        let budget = Self::new(shape, self.limits, self.clock.clone())?;
        for (target, source) in [
            (&budget.intermediate_rows, &self.intermediate_rows),
            (&budget.property_path_edges, &self.property_path_edges),
            (&budget.result_rows, &self.result_rows),
            (&budget.result_cells, &self.result_cells),
            (&budget.result_bytes, &self.result_bytes),
            (&budget.graph_triples, &self.graph_triples),
        ] {
            target.store(source.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        Ok(budget)
    }

    pub(crate) fn check(&self) -> std::result::Result<(), QueryLimitExceeded> {
        if let Some(error) = clock_error(&self.clock) {
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn clock(&self) -> &RequestClock {
        &self.clock
    }

    pub(crate) fn dense_cache_limits(&self) -> (usize, usize) {
        (
            self.limits.max_hash_entries.min(DENSE_CACHE_ENTRIES),
            self.limits.max_hash_bytes.min(DENSE_CACHE_BYTES),
        )
    }

    pub(crate) fn observe_intermediate(
        &self,
        rows: usize,
    ) -> std::result::Result<(), QueryLimitExceeded> {
        self.check()?;
        let total = add_limited(
            &self.intermediate_rows,
            rows,
            BudgetLimit {
                value: self.limits.max_intermediate_rows,
                resource: "intermediate rows",
            },
        )?;
        if self.shape.guarded_hash {
            if total > self.limits.max_hash_entries {
                return Err(QueryLimitExceeded::limit(
                    "hash entries",
                    self.limits.max_hash_entries,
                ));
            }
            let bytes = total.saturating_mul(HASH_ROW_ESTIMATE);
            if bytes > self.limits.max_hash_bytes {
                return Err(QueryLimitExceeded::limit(
                    "estimated hash bytes",
                    self.limits.max_hash_bytes,
                ));
            }
        }
        if self.shape.property_path {
            add_limited(
                &self.property_path_edges,
                rows,
                BudgetLimit {
                    value: self.limits.max_property_path_edges,
                    resource: "property path edges",
                },
            )?;
        }
        Ok(())
    }

    pub(crate) fn check_hash(
        &self,
        entries: usize,
        bytes: usize,
    ) -> std::result::Result<(), QueryLimitExceeded> {
        self.check()?;
        if entries > self.limits.max_hash_entries {
            return Err(QueryLimitExceeded::limit(
                "hash entries",
                self.limits.max_hash_entries,
            ));
        }
        if bytes > self.limits.max_hash_bytes {
            return Err(QueryLimitExceeded::limit(
                "hash bytes",
                self.limits.max_hash_bytes,
            ));
        }
        Ok(())
    }

    pub(crate) fn observe_solution(
        &self,
        row: &HashMap<String, EncodedTerm>,
    ) -> std::result::Result<(), QueryLimitExceeded> {
        self.observe_result(
            row.len(),
            row.iter().fold(
                row.capacity()
                    .saturating_mul(std::mem::size_of::<(String, EncodedTerm)>()),
                |bytes, (variable, term)| {
                    bytes
                        .saturating_add(variable.capacity())
                        .saturating_add(term.0.capacity())
                },
            ),
        )
    }

    pub(crate) fn observe_capacity<T>(
        &self,
        previous: usize,
        current: usize,
    ) -> std::result::Result<(), QueryLimitExceeded> {
        let added = current
            .saturating_sub(previous)
            .saturating_mul(std::mem::size_of::<T>());
        add_limited(
            &self.result_bytes,
            added,
            BudgetLimit {
                value: self.limits.max_result_bytes,
                resource: "estimated result bytes",
            },
        )?;
        Ok(())
    }

    pub(crate) fn observe_graph(
        &self,
        triple: &(EncodedTerm, EncodedTerm, EncodedTerm),
    ) -> std::result::Result<(), QueryLimitExceeded> {
        add_limited(
            &self.graph_triples,
            1,
            BudgetLimit {
                value: self.limits.max_graph_triples,
                resource: "graph triples",
            },
        )?;
        self.observe_result(
            3,
            triple
                .0
                .0
                .capacity()
                .saturating_add(triple.1.0.capacity())
                .saturating_add(triple.2.0.capacity()),
        )
    }

    pub(crate) fn observe_boolean(&self) -> std::result::Result<(), QueryLimitExceeded> {
        self.observe_result(1, 1)
    }

    fn observe_result(
        &self,
        cells: usize,
        bytes: usize,
    ) -> std::result::Result<(), QueryLimitExceeded> {
        self.check()?;
        add_limited(
            &self.result_rows,
            1,
            BudgetLimit {
                value: self.limits.max_result_rows,
                resource: "result rows",
            },
        )?;
        add_limited(
            &self.result_cells,
            cells,
            BudgetLimit {
                value: self.limits.max_result_cells,
                resource: "result cells",
            },
        )?;
        add_limited(
            &self.result_bytes,
            bytes,
            BudgetLimit {
                value: self.limits.max_result_bytes,
                resource: "estimated result bytes",
            },
        )?;
        Ok(())
    }
}

fn clock_error(clock: &RequestClock) -> Option<QueryLimitExceeded> {
    match clock.outcome() {
        RequestOutcome::Active => None,
        RequestOutcome::Explicit => Some(QueryLimitExceeded::Cancelled),
        RequestOutcome::Deadline => Some(QueryLimitExceeded::limit("query deadline", 0)),
        RequestOutcome::QueryCapacity => Some(QueryLimitExceeded::limit(
            "active query registrations",
            MAX_QUERY_REGISTRATIONS,
        )),
        RequestOutcome::DeadlineCapacity => Some(QueryLimitExceeded::limit(
            "active deadline registrations",
            MAX_DEADLINE_REGISTRATIONS,
        )),
        RequestOutcome::DeadlineUnavailable => {
            Some(QueryLimitExceeded::limit("deadline service", 0))
        }
    }
}

fn add_limited(
    counter: &AtomicUsize,
    amount: usize,
    limit: BudgetLimit,
) -> std::result::Result<usize, QueryLimitExceeded> {
    let previous = counter.fetch_add(amount, Ordering::Relaxed);
    let total = previous.saturating_add(amount);
    if total > limit.value {
        return Err(QueryLimitExceeded::limit(limit.resource, limit.value));
    }
    Ok(total)
}
