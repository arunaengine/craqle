use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::shacl::{ShaclValidationOptions, ShaclValidationReport, ShaclValidationStatistics};
use crate::store::{GraphStore, TermId, hash_term};
use crate::{EncodedTerm, GraphId, Result};

use super::constraints::{TermMetaCache, language_matches};
use super::model::{NodeKindPlan, ShapeId};
use super::paths::{self, PathWork};
use super::report::{PendingPath, ReportBuilder};
use super::resolve::{ResolvedConstraint, ResolvedPath, ResolvedSchema};
use super::targets::{TargetWork, resolve_targets};
use super::term_meta::TermKind;

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const QUALIFIED_MIN: &str = "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent";
const QUALIFIED_MAX: &str = "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent";

#[derive(Clone, Copy)]
enum CachedConformance {
    InProgress,
    Conforms,
    Violates,
}

pub(crate) fn validate(
    store: &GraphStore,
    schema: Arc<ResolvedSchema>,
    data_graph: &GraphId,
    options: &ShaclValidationOptions,
    resolved_cache_hit: bool,
    resolve_time: Duration,
    stop_after_first: bool,
) -> Result<ShaclValidationReport> {
    let view = StoreReadView::new(store);
    if !view.contains_graph(data_graph)? {
        return Err(crate::shacl::ShaclError::DataGraphNotFound {
            graph: data_graph.to_string(),
        }
        .into());
    }
    let context = ReadContext::for_validation(options.cancellation.clone(), data_graph);
    let graph_term = EncodedTerm::from_named_node(&data_graph.0);
    let graph = view
        .lookup_term(&context, &graph_term)?
        .unwrap_or_else(|| hash_term(&graph_term));
    let rdf_type_term = EncodedTerm(RDF_TYPE.to_owned());
    let rdf_type = view
        .lookup_term(&context, &rdf_type_term)?
        .unwrap_or_else(|| hash_term(&rdf_type_term));

    let target_start = Instant::now();
    let mut target_work = TargetWork::default();
    let targets = resolve_targets(&view, &context, graph, rdf_type, &schema, &mut target_work)?;
    let target_time = target_start.elapsed();

    let mut validator = Validator {
        view: &view,
        context: &context,
        graph,
        rdf_type,
        schema,
        options,
        report: ReportBuilder::new(options.max_results, stop_after_first),
        statistics: ShaclValidationStatistics {
            shape_compile_cache_hit: resolved_cache_hit,
            shapes_considered: targets.len() as u64,
            target_candidates: target_work.candidates,
            compile_time: resolve_time,
            target_time,
            ..ShaclValidationStatistics::default()
        },
        term_meta: TermMetaCache::default(),
        path_work: PathWork::default(),
        path_cache: HashMap::new(),
        conformance_cache: HashMap::new(),
        halted: false,
    };

    let constraint_start = Instant::now();
    for (shape_index, focus_nodes) in targets.iter().enumerate() {
        let shape_id = ShapeId(shape_index as u32);
        if validator.schema.portable.shapes[shape_index].deactivated {
            validator.statistics.shapes_skipped =
                validator.statistics.shapes_skipped.saturating_add(1);
            continue;
        }
        if focus_nodes.is_empty() {
            validator.statistics.shapes_skipped =
                validator.statistics.shapes_skipped.saturating_add(1);
            continue;
        }
        validator.statistics.shapes_executed =
            validator.statistics.shapes_executed.saturating_add(1);
        for focus in focus_nodes {
            validator.statistics.focus_nodes = validator.statistics.focus_nodes.saturating_add(1);
            let _ = validator.evaluate_shape(shape_id, *focus, true)?;
            if validator.halted {
                validator.statistics.stopped_early = true;
                break;
            }
        }
        if validator.halted {
            break;
        }
    }
    validator.statistics.constraint_time = constraint_start.elapsed();
    validator
        .report
        .finish(validator.view, validator.context, validator.statistics)
}

struct Validator<'view, 'context, 'options, V> {
    view: &'view V,
    context: &'context ReadContext<'context>,
    graph: TermId,
    rdf_type: TermId,
    schema: Arc<ResolvedSchema>,
    options: &'options ShaclValidationOptions,
    report: ReportBuilder,
    statistics: ShaclValidationStatistics,
    term_meta: TermMetaCache,
    path_work: PathWork,
    path_cache: HashMap<(ShapeId, TermId), Arc<BTreeSet<TermId>>>,
    conformance_cache: HashMap<(ShapeId, TermId), CachedConformance>,
    halted: bool,
}

impl<V: RdfReadView> Validator<'_, '_, '_, V> {
    fn evaluate_shape(&mut self, shape_id: ShapeId, focus: TermId, emit: bool) -> Result<bool> {
        let schema = self.schema.clone();
        let index = shape_id.0 as usize;
        let portable = &schema.portable.shapes[index];
        if portable.deactivated {
            return Ok(true);
        }
        if !emit {
            match self.conformance_cache.get(&(shape_id, focus)).copied() {
                Some(CachedConformance::Conforms) => return Ok(true),
                Some(CachedConformance::Violates) => return Ok(false),
                Some(CachedConformance::InProgress) => {
                    return Err(crate::shacl::ShaclError::CyclicShapeEvaluation {
                        shape: portable.label.0.clone(),
                        focus: format!("{:032x}", focus.0),
                    }
                    .into());
                }
                None => {}
            }
            self.conformance_cache
                .insert((shape_id, focus), CachedConformance::InProgress);
        }

        let mut conforms = true;
        for constraint in &schema.shapes[index].constraints {
            self.statistics.constraints_evaluated =
                self.statistics.constraints_evaluated.saturating_add(1);
            if !self.evaluate_constraint(shape_id, focus, constraint, emit)? {
                conforms = false;
                if !emit || self.halted {
                    break;
                }
            }
        }
        if !emit {
            self.conformance_cache.insert(
                (shape_id, focus),
                if conforms {
                    CachedConformance::Conforms
                } else {
                    CachedConformance::Violates
                },
            );
        }
        Ok(conforms)
    }

    fn evaluate_constraint(
        &mut self,
        shape_id: ShapeId,
        focus: TermId,
        constraint: &ResolvedConstraint,
        emit: bool,
    ) -> Result<bool> {
        match constraint {
            ResolvedConstraint::MinCount(minimum) => {
                let values = self.shape_values(shape_id, focus, Some(*minimum))?;
                let conforms = values.len() >= *minimum;
                if !conforms {
                    self.violate(shape_id, constraint, focus, None, None, None, emit)?;
                }
                Ok(conforms)
            }
            ResolvedConstraint::MaxCount(maximum) => {
                let cap = maximum.checked_add(1);
                let values = self.shape_values(shape_id, focus, cap)?;
                let conforms = values.len() <= *maximum;
                if !conforms {
                    self.violate(shape_id, constraint, focus, None, None, None, emit)?;
                }
                Ok(conforms)
            }
            ResolvedConstraint::HasValue(expected) => {
                let conforms = self.has_value(shape_id, focus, *expected)?;
                if !conforms {
                    self.violate(shape_id, constraint, focus, None, None, None, emit)?;
                }
                Ok(conforms)
            }
            ResolvedConstraint::Closed { ignored_properties } => {
                self.evaluate_closed(shape_id, focus, constraint, ignored_properties, emit)
            }
            ResolvedConstraint::QualifiedValueShape {
                shape,
                min_count,
                max_count,
                disjoint,
                siblings,
            } => {
                let values = self.shape_values(shape_id, focus, None)?;
                let mut count = 0usize;
                for value in values.iter().copied() {
                    if !self.evaluate_shape(*shape, value, false)? {
                        continue;
                    }
                    if *disjoint {
                        let mut sibling_match = false;
                        for sibling in siblings {
                            if self.evaluate_shape(*sibling, value, false)? {
                                sibling_match = true;
                                break;
                            }
                        }
                        if sibling_match {
                            continue;
                        }
                    }
                    count = count.saturating_add(1);
                }
                let mut conforms = true;
                if min_count.is_some_and(|minimum| count < minimum) {
                    conforms = false;
                    self.violate(
                        shape_id,
                        constraint,
                        focus,
                        None,
                        None,
                        Some(QUALIFIED_MIN),
                        emit,
                    )?;
                }
                if max_count.is_some_and(|maximum| count > maximum) {
                    conforms = false;
                    self.violate(
                        shape_id,
                        constraint,
                        focus,
                        None,
                        None,
                        Some(QUALIFIED_MAX),
                        emit,
                    )?;
                }
                Ok(conforms)
            }
            _ => self.evaluate_value_constraint(shape_id, focus, constraint, emit),
        }
    }

    fn evaluate_value_constraint(
        &mut self,
        shape_id: ShapeId,
        focus: TermId,
        constraint: &ResolvedConstraint,
        emit: bool,
    ) -> Result<bool> {
        let values = self.shape_values(shape_id, focus, None)?;
        let mut conforms = true;
        match constraint {
            ResolvedConstraint::Class(class) => {
                for value in values.iter().copied() {
                    if !self.view.exists(
                        self.context,
                        GraphSelector::Named(self.graph),
                        QuadPattern {
                            subject: Some(value),
                            predicate: Some(self.rdf_type),
                            object: Some(*class),
                            ..QuadPattern::default()
                        },
                    )? {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Datatype(datatype) => {
                for value in values.iter().copied() {
                    let meta = self.term_meta.get(self.view, self.context, value)?.cloned();
                    if meta.as_ref().and_then(|meta| meta.datatype) != Some(*datatype) {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::NodeKind(kind) => {
                for value in values.iter().copied() {
                    let actual = self
                        .term_meta
                        .get(self.view, self.context, value)?
                        .map(|meta| meta.kind);
                    if !node_kind_matches(*kind, actual) {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::MinExclusive(boundary)
            | ResolvedConstraint::MaxExclusive(boundary)
            | ResolvedConstraint::MinInclusive(boundary)
            | ResolvedConstraint::MaxInclusive(boundary) => {
                for value in values.iter().copied() {
                    let comparison = self
                        .term_meta
                        .get(self.view, self.context, value)?
                        .and_then(|meta| meta.partial_cmp(boundary));
                    let valid = match constraint {
                        ResolvedConstraint::MinExclusive(_) => {
                            comparison == Some(Ordering::Greater)
                        }
                        ResolvedConstraint::MaxExclusive(_) => comparison == Some(Ordering::Less),
                        ResolvedConstraint::MinInclusive(_) => {
                            matches!(comparison, Some(Ordering::Greater | Ordering::Equal))
                        }
                        ResolvedConstraint::MaxInclusive(_) => {
                            matches!(comparison, Some(Ordering::Less | Ordering::Equal))
                        }
                        _ => unreachable!(),
                    };
                    if !valid {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::MinLength(minimum) | ResolvedConstraint::MaxLength(minimum) => {
                for value in values.iter().copied() {
                    let length = self
                        .term_meta
                        .get(self.view, self.context, value)?
                        .filter(|meta| meta.lexical.is_some())
                        .map(|meta| meta.lexical_length);
                    let valid = match constraint {
                        ResolvedConstraint::MinLength(_) => {
                            length.is_some_and(|length| length >= *minimum)
                        }
                        ResolvedConstraint::MaxLength(_) => {
                            length.is_some_and(|length| length <= *minimum)
                        }
                        _ => unreachable!(),
                    };
                    if !valid {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Pattern(pattern) => {
                for value in values.iter().copied() {
                    let matches = self
                        .term_meta
                        .get(self.view, self.context, value)?
                        .and_then(|meta| meta.lexical.as_deref())
                        .is_some_and(|lexical| pattern.is_match(lexical));
                    if !matches {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::UniqueLang(required) => {
                if *required {
                    let mut seen = HashMap::new();
                    let mut reported = BTreeSet::new();
                    for value in values.iter().copied() {
                        let language = self
                            .term_meta
                            .get(self.view, self.context, value)?
                            .and_then(|meta| meta.language.clone());
                        if let Some(language) = language
                            && seen.insert(language.clone(), value).is_some()
                            && reported.insert(language)
                        {
                            conforms = false;
                            self.violate(shape_id, constraint, focus, None, None, None, emit)?;
                            if self.halted || !emit {
                                break;
                            }
                        }
                    }
                }
            }
            ResolvedConstraint::LanguageIn(ranges) => {
                for value in values.iter().copied() {
                    let valid = self
                        .term_meta
                        .get(self.view, self.context, value)?
                        .and_then(|meta| meta.language.as_deref())
                        .is_some_and(|language| language_matches(language, ranges));
                    if !valid {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::In(allowed) => {
                for value in values.iter().copied() {
                    if !allowed.contains(&value) {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Equals(predicate) => {
                let other = self.predicate_values(focus, *predicate)?;
                for value in values.symmetric_difference(&other).copied() {
                    conforms = false;
                    self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                    if self.halted || !emit {
                        break;
                    }
                }
            }
            ResolvedConstraint::Disjoint(predicate) => {
                let other = self.predicate_values(focus, *predicate)?;
                for value in values.intersection(&other).copied() {
                    conforms = false;
                    self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                    if self.halted || !emit {
                        break;
                    }
                }
            }
            ResolvedConstraint::LessThan(predicate)
            | ResolvedConstraint::LessThanOrEquals(predicate) => {
                let other = self.predicate_values(focus, *predicate)?;
                for value in values.iter().copied() {
                    let left = self.term_meta.get(self.view, self.context, value)?.cloned();
                    let mut valid = left.is_some();
                    if let Some(left) = left {
                        for right in &other {
                            let comparison = self
                                .term_meta
                                .get(self.view, self.context, *right)?
                                .and_then(|right| left.partial_cmp(right));
                            let ordered = match constraint {
                                ResolvedConstraint::LessThan(_) => {
                                    comparison == Some(Ordering::Less)
                                }
                                ResolvedConstraint::LessThanOrEquals(_) => {
                                    matches!(comparison, Some(Ordering::Less | Ordering::Equal))
                                }
                                _ => unreachable!(),
                            };
                            if !ordered {
                                valid = false;
                                break;
                            }
                        }
                    }
                    if !valid {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Node(shape) => {
                for value in values.iter().copied() {
                    if !self.evaluate_shape(*shape, value, false)? {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Not(shape) => {
                for value in values.iter().copied() {
                    if self.evaluate_shape(*shape, value, false)? {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::And(shapes) | ResolvedConstraint::Or(shapes) => {
                for value in values.iter().copied() {
                    let mut matches = 0usize;
                    for shape in shapes {
                        if self.evaluate_shape(*shape, value, false)? {
                            matches += 1;
                        }
                    }
                    let valid = match constraint {
                        ResolvedConstraint::And(_) => matches == shapes.len(),
                        ResolvedConstraint::Or(_) => matches > 0,
                        _ => unreachable!(),
                    };
                    if !valid {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::Xone(shapes) => {
                for value in values.iter().copied() {
                    let mut matches = 0usize;
                    for shape in shapes {
                        if self.evaluate_shape(*shape, value, false)? {
                            matches += 1;
                        }
                    }
                    if matches != 1 {
                        conforms = false;
                        self.violate(shape_id, constraint, focus, Some(value), None, None, emit)?;
                        if self.halted || !emit {
                            break;
                        }
                    }
                }
            }
            ResolvedConstraint::MinCount(_)
            | ResolvedConstraint::MaxCount(_)
            | ResolvedConstraint::HasValue(_)
            | ResolvedConstraint::QualifiedValueShape { .. }
            | ResolvedConstraint::Closed { .. } => unreachable!(),
        }
        Ok(conforms)
    }

    fn shape_values(
        &mut self,
        shape_id: ShapeId,
        focus: TermId,
        cap: Option<usize>,
    ) -> Result<Arc<BTreeSet<TermId>>> {
        let schema = self.schema.clone();
        let Some(path) = schema.shapes[shape_id.0 as usize].path.as_ref() else {
            return Ok(Arc::new(BTreeSet::from([focus])));
        };
        if cap.is_none()
            && let Some(values) = self.path_cache.get(&(shape_id, focus))
        {
            return Ok(values.clone());
        }
        let before = self.context.snapshot();
        let values = Arc::new(paths::values(
            self.view,
            self.context,
            self.graph,
            focus,
            path,
            cap,
            self.options,
            &mut self.path_work,
        )?);
        let after = self.context.snapshot();
        self.statistics.path_index_seeks = self
            .statistics
            .path_index_seeks
            .saturating_add(after.index_seeks.saturating_sub(before.index_seeks));
        self.statistics.path_candidate_quads = self
            .statistics
            .path_candidate_quads
            .saturating_add(after.candidate_quads.saturating_sub(before.candidate_quads));
        if cap.is_none() {
            self.path_cache.insert((shape_id, focus), values.clone());
        }
        Ok(values)
    }

    fn has_value(&mut self, shape_id: ShapeId, focus: TermId, expected: TermId) -> Result<bool> {
        let schema = self.schema.clone();
        match schema.shapes[shape_id.0 as usize].path.as_ref() {
            None => Ok(focus == expected),
            Some(ResolvedPath::Predicate(predicate)) => Ok(self.view.exists(
                self.context,
                GraphSelector::Named(self.graph),
                QuadPattern {
                    subject: Some(focus),
                    predicate: Some(*predicate),
                    object: Some(expected),
                    ..QuadPattern::default()
                },
            )?),
            Some(ResolvedPath::Inverse(path)) => match path.as_ref() {
                ResolvedPath::Predicate(predicate) => Ok(self.view.exists(
                    self.context,
                    GraphSelector::Named(self.graph),
                    QuadPattern {
                        subject: Some(expected),
                        predicate: Some(*predicate),
                        object: Some(focus),
                        ..QuadPattern::default()
                    },
                )?),
                _ => Ok(self
                    .shape_values(shape_id, focus, None)?
                    .contains(&expected)),
            },
            Some(_) => Ok(self
                .shape_values(shape_id, focus, None)?
                .contains(&expected)),
        }
    }

    fn predicate_values(&mut self, focus: TermId, predicate: TermId) -> Result<BTreeSet<TermId>> {
        let mut values = BTreeSet::new();
        let cursor = self.view.forward_predicate(
            self.context,
            GraphSelector::Named(self.graph),
            focus,
            predicate,
        )?;
        for quad in cursor {
            values.insert(quad?.object);
        }
        Ok(values)
    }

    fn evaluate_closed(
        &mut self,
        shape_id: ShapeId,
        focus: TermId,
        constraint: &ResolvedConstraint,
        ignored_properties: &BTreeSet<TermId>,
        emit: bool,
    ) -> Result<bool> {
        let schema = self.schema.clone();
        let mut allowed = ignored_properties.clone();
        for property in &schema.portable.shapes[shape_id.0 as usize].property_shapes {
            if let Some(ResolvedPath::Predicate(predicate)) =
                schema.shapes[property.0 as usize].path.as_ref()
            {
                allowed.insert(*predicate);
            }
        }
        let mut conforms = true;
        let cursor = self.view.scan(
            self.context,
            GraphSelector::Named(self.graph),
            QuadPattern {
                subject: Some(focus),
                ..QuadPattern::default()
            },
        )?;
        for quad in cursor {
            let quad = quad?;
            if !allowed.contains(&quad.predicate) {
                conforms = false;
                self.violate(
                    shape_id,
                    constraint,
                    focus,
                    Some(quad.object),
                    Some(PendingPath::Predicate(quad.predicate)),
                    None,
                    emit,
                )?;
                if self.halted || !emit {
                    break;
                }
            }
        }
        Ok(conforms)
    }

    #[allow(clippy::too_many_arguments)]
    fn violate(
        &mut self,
        shape_id: ShapeId,
        constraint: &ResolvedConstraint,
        focus: TermId,
        value: Option<TermId>,
        path: Option<PendingPath>,
        component: Option<&'static str>,
        emit: bool,
    ) -> Result<()> {
        if !emit {
            return Ok(());
        }
        let schema = self.schema.clone();
        let shape = &schema.portable.shapes[shape_id.0 as usize];
        self.halted = match component {
            Some(component) => self
                .report
                .emit_with_component(shape, component, focus, value, path)?,
            None => self.report.emit(shape, constraint, focus, value, path)?,
        };
        Ok(())
    }
}

fn node_kind_matches(expected: NodeKindPlan, actual: Option<TermKind>) -> bool {
    match expected {
        NodeKindPlan::Iri => actual == Some(TermKind::Iri),
        NodeKindPlan::Literal => actual == Some(TermKind::Literal),
        NodeKindPlan::BlankNode => actual == Some(TermKind::BlankNode),
        NodeKindPlan::BlankNodeOrIri => {
            matches!(actual, Some(TermKind::BlankNode | TermKind::Iri))
        }
        NodeKindPlan::BlankNodeOrLiteral => {
            matches!(actual, Some(TermKind::BlankNode | TermKind::Literal))
        }
        NodeKindPlan::IriOrLiteral => matches!(actual, Some(TermKind::Iri | TermKind::Literal)),
    }
}
