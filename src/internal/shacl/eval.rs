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
