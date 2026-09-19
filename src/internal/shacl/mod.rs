//! Organizes native SHACL compilation and validation.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

pub(crate) mod compile;
pub(crate) mod constraints;
pub(crate) mod dependencies;
pub(crate) mod eval;
pub(crate) mod model;
pub(crate) mod paths;
pub(crate) mod report;
pub(crate) mod resolve;
pub(crate) mod targets;
pub(crate) mod term_meta;

pub(crate) use compile::ShaclCompiler;
