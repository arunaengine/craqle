//! Owns query read state, index cursors, and bounded parallel execution.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

pub(crate) mod budget;
pub(crate) mod context;
pub(crate) mod cursor;
pub(crate) mod deadline;
pub(crate) mod worker;
