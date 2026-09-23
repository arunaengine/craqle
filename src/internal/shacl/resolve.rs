//! Resolves SHACL shapes and their local import dependencies.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::BTreeSet;
use std::mem::size_of;
use std::sync::Arc;

use regex::{Regex, RegexBuilder};

use crate::shacl::ShaclError;
use crate::store::{GraphStore, TermId, hash_term};
use crate::{EncodedTerm, Result};

use super::model::{
    CompiledSchemaInner, ConstraintPlan, NodeKindPlan, PathPlan, ShapeId, TargetPlan,
};
use super::term_meta::TermMeta;

const ALLOCATION_RESERVE: usize = 2 * size_of::<usize>();
/// Regex does not expose engine allocations; reserve combined opaque retained state.
const REGEX_RESERVE: usize = 16 * 1_048_576;

fn slice_bytes<T>(slice: &[T]) -> usize {
    slice
        .len()
        .saturating_mul(size_of::<T>())
        .saturating_add(ALLOCATION_RESERVE)
}

fn sum_bytes(values: impl Iterator<Item = usize>) -> usize {
    values.fold(0, usize::saturating_add)
}

pub(crate) struct ResolvedSchema {
    pub(crate) portable: Arc<CompiledSchemaInner>,
    pub(crate) shapes: Box<[ResolvedShape]>,
}

pub(crate) struct ResolvedShape {
    pub(crate) targets: Box<[ResolvedTarget]>,
    pub(crate) path: Option<ResolvedPath>,
    pub(crate) constraints: Box<[ResolvedConstraint]>,
}

pub(crate) enum ResolvedTarget {
    Node(TermId),
    Class(TermId),
    SubjectsOf(TermId),
    ObjectsOf(TermId),
    ImplicitClass(TermId),
}

pub(crate) enum ResolvedPath {
    Predicate(TermId),
    Alternative(Box<[ResolvedPath]>),
    Sequence(Box<[ResolvedPath]>),
    Inverse(Box<ResolvedPath>),
    ZeroOrMore(Box<ResolvedPath>),
    OneOrMore(Box<ResolvedPath>),
    ZeroOrOne(Box<ResolvedPath>),
}

pub(crate) enum ResolvedConstraint {
    Class(TermId),
    Datatype(TermId),
    NodeKind(NodeKindPlan),
    MinCount(usize),
    MaxCount(usize),
    MinExclusive(TermMeta),
    MaxExclusive(TermMeta),
    MinInclusive(TermMeta),
    MaxInclusive(TermMeta),
    MinLength(usize),
    MaxLength(usize),
    Pattern(Regex),
    UniqueLang(bool),
    LanguageIn(Box<[String]>),
    Equals(TermId),
    Disjoint(TermId),
    LessThan(TermId),
    LessOrEqual(TermId),
    Or(Box<[ShapeId]>),
    And(Box<[ShapeId]>),
    Not(ShapeId),
    Xone(Box<[ShapeId]>),
    Node(ShapeId),
    HasValue(TermId),
    In(BTreeSet<TermId>),
    QualifiedValueShape {
        shape: ShapeId,
        min_count: Option<usize>,
        max_count: Option<usize>,
        disjoint: bool,
        siblings: Box<[ShapeId]>,
    },
    Closed {
        ignored_properties: BTreeSet<TermId>,
    },
}

impl ResolvedSchema {
    /// Conservative cache admission estimate, not a process RSS measurement.
    pub(crate) fn estimated_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(ALLOCATION_RESERVE)
            // Charge shared portable backing here because this cache may be its sole owner.
            .saturating_add(self.portable.estimated_bytes())
            .saturating_add(slice_bytes(&self.shapes))
            .saturating_add(sum_bytes(
                self.shapes.iter().map(ResolvedShape::estimated_bytes),
            ))
    }
}

impl ResolvedShape {
    fn estimated_bytes(&self) -> usize {
        slice_bytes(&self.targets)
            .saturating_add(
                self.path
                    .as_ref()
                    .map(ResolvedPath::estimated_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(slice_bytes(&self.constraints))
            .saturating_add(sum_bytes(
                self.constraints
                    .iter()
                    .map(ResolvedConstraint::estimated_bytes),
            ))
    }
}

impl ResolvedPath {
    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Predicate(_) => 0,
            Self::Alternative(paths) | Self::Sequence(paths) => slice_bytes(paths)
                .saturating_add(sum_bytes(paths.iter().map(Self::estimated_bytes))),
            Self::Inverse(path)
            | Self::ZeroOrMore(path)
            | Self::OneOrMore(path)
            | Self::ZeroOrOne(path) => size_of::<Self>()
                .saturating_add(ALLOCATION_RESERVE)
                .saturating_add(path.estimated_bytes()),
        }
    }
}

impl ResolvedConstraint {
    fn estimated_bytes(&self) -> usize {
        match self {
            Self::MinExclusive(meta)
            | Self::MaxExclusive(meta)
            | Self::MinInclusive(meta)
            | Self::MaxInclusive(meta) => meta.estimated_bytes(),
            Self::Pattern(regex) => REGEX_RESERVE.saturating_add(regex.as_str().len()),
            Self::LanguageIn(values) => slice_bytes(values).saturating_add(sum_bytes(
                values
                    .iter()
                    .map(|value| value.capacity().saturating_add(ALLOCATION_RESERVE)),
            )),
            Self::Or(shapes) | Self::And(shapes) | Self::Xone(shapes) => slice_bytes(shapes),
            Self::In(terms)
            | Self::Closed {
                ignored_properties: terms,
            } => terms
                .len()
                .saturating_mul(size_of::<TermId>().saturating_add(4 * size_of::<usize>()))
                .saturating_add(ALLOCATION_RESERVE),
            Self::QualifiedValueShape { siblings, .. } => slice_bytes(siblings),
            Self::Class(_)
            | Self::Datatype(_)
            | Self::NodeKind(_)
            | Self::MinCount(_)
            | Self::MaxCount(_)
            | Self::MinLength(_)
            | Self::MaxLength(_)
            | Self::UniqueLang(_)
            | Self::Equals(_)
            | Self::Disjoint(_)
            | Self::LessThan(_)
            | Self::LessOrEqual(_)
            | Self::Not(_)
            | Self::Node(_)
            | Self::HasValue(_) => 0,
        }
    }
}

pub(crate) fn resolve(
    store: &GraphStore,
    portable: Arc<CompiledSchemaInner>,
) -> Result<ResolvedSchema> {
    let shapes = portable
        .shapes
        .iter()
        .map(|shape| {
            let targets = shape
                .targets
                .iter()
                .map(|target| resolve_target(store, target))
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice();
            let path = shape
                .path
                .as_ref()
                .map(|path| resolve_path(store, path))
                .transpose()?;
            let constraints = shape
                .constraints
                .iter()
                .map(|constraint| resolve_constraint(store, &shape.label, constraint))
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice();
            Ok(ResolvedShape {
                targets,
                path,
                constraints,
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    Ok(ResolvedSchema { portable, shapes })
}

fn resolve_target(store: &GraphStore, target: &TargetPlan) -> Result<ResolvedTarget> {
    Ok(match target {
        TargetPlan::Node(term) => ResolvedTarget::Node(resolve_term(store, term)?),
        TargetPlan::Class(term) => ResolvedTarget::Class(resolve_term(store, term)?),
        TargetPlan::SubjectsOf(term) => ResolvedTarget::SubjectsOf(resolve_term(store, term)?),
        TargetPlan::ObjectsOf(term) => ResolvedTarget::ObjectsOf(resolve_term(store, term)?),
        TargetPlan::ImplicitClass(term) => {
            ResolvedTarget::ImplicitClass(resolve_term(store, term)?)
        }
    })
}

fn resolve_path(store: &GraphStore, path: &PathPlan) -> Result<ResolvedPath> {
    Ok(match path {
        PathPlan::Predicate(predicate) => ResolvedPath::Predicate(resolve_term(store, predicate)?),
        PathPlan::Alternative(paths) => ResolvedPath::Alternative(
            paths
                .iter()
                .map(|path| resolve_path(store, path))
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
        ),
        PathPlan::Sequence(paths) => ResolvedPath::Sequence(
            paths
                .iter()
                .map(|path| resolve_path(store, path))
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
        ),
        PathPlan::Inverse(path) => ResolvedPath::Inverse(Box::new(resolve_path(store, path)?)),
        PathPlan::ZeroOrMore(path) => {
            ResolvedPath::ZeroOrMore(Box::new(resolve_path(store, path)?))
        }
        PathPlan::OneOrMore(path) => ResolvedPath::OneOrMore(Box::new(resolve_path(store, path)?)),
        PathPlan::ZeroOrOne(path) => ResolvedPath::ZeroOrOne(Box::new(resolve_path(store, path)?)),
    })
}

fn resolve_constraint(
    store: &GraphStore,
    shape: &EncodedTerm,
    constraint: &ConstraintPlan,
) -> Result<ResolvedConstraint> {
    Ok(match constraint {
        ConstraintPlan::Class(term) => ResolvedConstraint::Class(resolve_term(store, term)?),
        ConstraintPlan::Datatype(term) => ResolvedConstraint::Datatype(resolve_term(store, term)?),
        ConstraintPlan::NodeKind(kind) => ResolvedConstraint::NodeKind(*kind),
        ConstraintPlan::MinCount(count) => ResolvedConstraint::MinCount(*count),
        ConstraintPlan::MaxCount(count) => ResolvedConstraint::MaxCount(*count),
        ConstraintPlan::MinExclusive(term) => {
            ResolvedConstraint::MinExclusive(resolve_boundary(shape, term)?)
        }
        ConstraintPlan::MaxExclusive(term) => {
            ResolvedConstraint::MaxExclusive(resolve_boundary(shape, term)?)
        }
        ConstraintPlan::MinInclusive(term) => {
            ResolvedConstraint::MinInclusive(resolve_boundary(shape, term)?)
        }
        ConstraintPlan::MaxInclusive(term) => {
            ResolvedConstraint::MaxInclusive(resolve_boundary(shape, term)?)
        }
        ConstraintPlan::MinLength(length) => ResolvedConstraint::MinLength(*length),
        ConstraintPlan::MaxLength(length) => ResolvedConstraint::MaxLength(*length),
        ConstraintPlan::Pattern { pattern, flags } => ResolvedConstraint::Pattern(compile_pattern(
            shape,
            pattern,
            flags.as_deref().unwrap_or_default(),
        )?),
        ConstraintPlan::UniqueLang(value) => ResolvedConstraint::UniqueLang(*value),
        ConstraintPlan::LanguageIn(languages) => ResolvedConstraint::LanguageIn(
            languages
                .iter()
                .map(|language| language.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
        ConstraintPlan::Equals(predicate) => {
            ResolvedConstraint::Equals(resolve_term(store, predicate)?)
        }
        ConstraintPlan::Disjoint(predicate) => {
            ResolvedConstraint::Disjoint(resolve_term(store, predicate)?)
        }
        ConstraintPlan::LessThan(predicate) => {
            ResolvedConstraint::LessThan(resolve_term(store, predicate)?)
        }
        ConstraintPlan::LessOrEqual(predicate) => {
            ResolvedConstraint::LessOrEqual(resolve_term(store, predicate)?)
        }
        ConstraintPlan::Or(shapes) => ResolvedConstraint::Or(shapes.clone()),
        ConstraintPlan::And(shapes) => ResolvedConstraint::And(shapes.clone()),
        ConstraintPlan::Not(shape) => ResolvedConstraint::Not(*shape),
        ConstraintPlan::Xone(shapes) => ResolvedConstraint::Xone(shapes.clone()),
        ConstraintPlan::Node(shape) => ResolvedConstraint::Node(*shape),
        ConstraintPlan::HasValue(term) => ResolvedConstraint::HasValue(resolve_term(store, term)?),
        ConstraintPlan::In(terms) => ResolvedConstraint::In(
            terms
                .iter()
                .map(|term| resolve_term(store, term))
                .collect::<Result<BTreeSet<_>>>()?,
        ),
        ConstraintPlan::QualifiedValueShape {
            shape,
            min_count,
            max_count,
            disjoint,
            siblings,
        } => ResolvedConstraint::QualifiedValueShape {
            shape: *shape,
            min_count: *min_count,
            max_count: *max_count,
            disjoint: *disjoint,
            siblings: siblings.clone(),
        },
        ConstraintPlan::Closed { ignored_properties } => ResolvedConstraint::Closed {
            ignored_properties: ignored_properties
                .iter()
                .map(|term| resolve_term(store, term))
                .collect::<Result<BTreeSet<_>>>()?,
        },
    })
}

fn resolve_term(store: &GraphStore, term: &EncodedTerm) -> Result<TermId> {
    Ok(store.lookup_term(term)?.unwrap_or_else(|| hash_term(term)))
}

fn resolve_boundary(shape: &EncodedTerm, term: &EncodedTerm) -> Result<TermMeta> {
    TermMeta::from_encoded(term).ok_or_else(|| {
        ShaclError::IllFormedShapes {
            graph: shape.0.clone(),
            message: format!("comparison boundary {} is not an RDF term", term.0),
        }
        .into()
    })
}

fn compile_pattern(shape: &EncodedTerm, pattern: &str, flags: &str) -> Result<Regex> {
    if let Some(flag) = flags
        .chars()
        .find(|flag| !matches!(flag, 'i' | 'm' | 's' | 'x' | 'q'))
    {
        return Err(ShaclError::InvalidPattern {
            shape: shape.0.clone(),
            pattern: pattern.to_owned(),
            flags: flags.to_owned(),
            message: format!("unsupported regular-expression flag `{flag}`"),
        }
        .into());
    }
    let quoted = flags.contains('q');
    let source = if quoted {
        regex::escape(pattern)
    } else {
        pattern.to_owned()
    };
    RegexBuilder::new(&source)
        .case_insensitive(flags.contains('i'))
        .multi_line(flags.contains('m'))
        .dot_matches_new_line(flags.contains('s'))
        .ignore_whitespace(flags.contains('x'))
        .build()
        .map_err(|error| {
            ShaclError::InvalidPattern {
                shape: shape.0.clone(),
                pattern: pattern.to_owned(),
                flags: flags.to_owned(),
                message: error.to_string(),
            }
            .into()
        })
}
