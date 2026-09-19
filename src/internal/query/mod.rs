//! Owns query read state, index cursors, and bounded parallel execution.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[allow(dead_code)]
pub(crate) mod context;
#[allow(dead_code)]
pub(crate) mod cursor;
pub(crate) mod worker;
