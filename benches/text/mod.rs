//! Experimental persisted text indexes used only by benchmark targets.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

mod engine;
mod tantivy_ref;

pub use engine::{Result, stable_key};

pub use engine::{
    ActiveStats, DocumentInput, DocumentKey, EngineError, EngineOptions, FjallBm25, HitIdentity,
    PostingLayout, QueryBounds, ScoreRequest, SearchHit, SearchReport, SearchRequest, StatsRequest,
    UpdateWork, WorkCount,
};
pub use tantivy_ref::{TantivyRequest, collect_full, collect_pruned};
