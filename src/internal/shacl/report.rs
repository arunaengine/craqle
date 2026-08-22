use std::time::Instant;

use crate::query_context::ReadContext;
use crate::rdf_read::RdfReadView;
use crate::shacl::{
    ShaclBlockingSeverity, ShaclError, ShaclMessage, ShaclValidationReport, ShaclValidationResult,
    ShaclValidationStatistics,
};
use crate::store::TermId;
use crate::{EncodedTerm, Result};

use super::model::{CompiledShape, PathPlan, SeverityPlan};
use super::resolve::ResolvedConstraint;

const SH: &str = "http://www.w3.org/ns/shacl#";

pub(crate) enum PendingPath {
    Portable(String),
    Predicate(TermId),
}

struct PendingResult {
    focus: TermId,
    value: Option<TermId>,
    result_path: Option<PendingPath>,
    source_shape: EncodedTerm,
    component: &'static str,
    severity: EncodedTerm,
    messages: Vec<ShaclMessage>,
}

pub(crate) struct ReportBuilder {
    results: Vec<PendingResult>,
    max_results: usize,
    stop_after_first: bool,
}

impl ReportBuilder {
    pub(crate) fn new(max_results: usize, stop_after_first: bool) -> Self {
        Self {
            results: Vec::new(),
            max_results,
            stop_after_first,
        }
    }

    pub(crate) fn emit(
        &mut self,
        shape: &CompiledShape,
        constraint: &ResolvedConstraint,
        focus: TermId,
        value: Option<TermId>,
        result_path: Option<PendingPath>,
    ) -> Result<bool> {
        self.emit_with_component(shape, component_iri(constraint), focus, value, result_path)
    }

    pub(crate) fn emit_with_component(
        &mut self,
        shape: &CompiledShape,
        component: &'static str,
        focus: TermId,
        value: Option<TermId>,
        result_path: Option<PendingPath>,
    ) -> Result<bool> {
        if self.results.len() >= self.max_results {
            return Err(ShaclError::ResultLimitExceeded {
                limit: self.max_results,
            }
            .into());
        }
        self.results.push(PendingResult {
            focus,
            value,
            result_path: result_path.or_else(|| {
                shape
                    .path
                    .as_ref()
                    .map(|path| PendingPath::Portable(path_label(path)))
            }),
            source_shape: shape.label.clone(),
            component,
            severity: severity_term(&shape.severity),
            messages: shape
                .messages
                .iter()
                .map(|message| ShaclMessage {
                    language: message.language.clone(),
                    text: message.text.clone(),
                })
                .collect(),
        });
        Ok(self.stop_after_first)
    }

    pub(crate) fn finish<V: RdfReadView + ?Sized>(
        self,
        view: &V,
        context: &ReadContext<'_>,
        mut statistics: ShaclValidationStatistics,
        blocking_severity: ShaclBlockingSeverity,
    ) -> Result<ShaclValidationReport> {
        let start = Instant::now();
        let mut results = Vec::with_capacity(self.results.len());
        for result in self.results {
            let result_path = match result.result_path {
                Some(PendingPath::Portable(path)) => Some(path),
                Some(PendingPath::Predicate(predicate)) => {
                    Some(view.decode_term(context, predicate)?.0)
                }
                None => None,
            };
            results.push(ShaclValidationResult {
                focus_node: view.decode_term(context, result.focus)?,
                value: result
                    .value
                    .map(|value| view.decode_term(context, value))
                    .transpose()?,
                result_path,
                source_shape: result.source_shape,
                source_constraint_component: result.component.to_owned(),
                severity: result.severity,
                messages: result.messages,
            });
        }
        results.sort();
        statistics.violations = results.len() as u64;
        statistics.report_time = start.elapsed();
        statistics.read = context.snapshot();
        statistics.terms_decoded = statistics.read.terms_decoded;
        let mut report = ShaclValidationReport {
            conforms: false,
            accepted_by_write_policy: false,
            results,
            statistics,
        };
        report.refresh_outcomes(blocking_severity);
        Ok(report)
    }
}

pub(crate) fn component_iri(constraint: &ResolvedConstraint) -> &'static str {
    match constraint {
        ResolvedConstraint::Class(_) => {
            concat!("http://www.w3.org/ns/shacl#", "ClassConstraintComponent")
        }
        ResolvedConstraint::Datatype(_) => {
            concat!("http://www.w3.org/ns/shacl#", "DatatypeConstraintComponent")
        }
        ResolvedConstraint::NodeKind(_) => {
            concat!("http://www.w3.org/ns/shacl#", "NodeKindConstraintComponent")
        }
        ResolvedConstraint::MinCount(_) => {
            concat!("http://www.w3.org/ns/shacl#", "MinCountConstraintComponent")
        }
        ResolvedConstraint::MaxCount(_) => {
            concat!("http://www.w3.org/ns/shacl#", "MaxCountConstraintComponent")
        }
        ResolvedConstraint::MinExclusive(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MinExclusiveConstraintComponent"
        ),
        ResolvedConstraint::MaxExclusive(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MaxExclusiveConstraintComponent"
        ),
        ResolvedConstraint::MinInclusive(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MinInclusiveConstraintComponent"
        ),
        ResolvedConstraint::MaxInclusive(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MaxInclusiveConstraintComponent"
        ),
        ResolvedConstraint::MinLength(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MinLengthConstraintComponent"
        ),
        ResolvedConstraint::MaxLength(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "MaxLengthConstraintComponent"
        ),
        ResolvedConstraint::Pattern(_) => {
            concat!("http://www.w3.org/ns/shacl#", "PatternConstraintComponent")
        }
        ResolvedConstraint::UniqueLang(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "UniqueLangConstraintComponent"
        ),
        ResolvedConstraint::LanguageIn(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "LanguageInConstraintComponent"
        ),
        ResolvedConstraint::Equals(_) => {
            concat!("http://www.w3.org/ns/shacl#", "EqualsConstraintComponent")
        }
        ResolvedConstraint::Disjoint(_) => {
            concat!("http://www.w3.org/ns/shacl#", "DisjointConstraintComponent")
        }
        ResolvedConstraint::LessThan(_) => {
            concat!("http://www.w3.org/ns/shacl#", "LessThanConstraintComponent")
        }
        ResolvedConstraint::LessThanOrEquals(_) => concat!(
            "http://www.w3.org/ns/shacl#",
            "LessThanOrEqualsConstraintComponent"
        ),
        ResolvedConstraint::Or(_) => {
            concat!("http://www.w3.org/ns/shacl#", "OrConstraintComponent")
        }
        ResolvedConstraint::And(_) => {
            concat!("http://www.w3.org/ns/shacl#", "AndConstraintComponent")
        }
        ResolvedConstraint::Not(_) => {
            concat!("http://www.w3.org/ns/shacl#", "NotConstraintComponent")
        }
        ResolvedConstraint::Xone(_) => {
            concat!("http://www.w3.org/ns/shacl#", "XoneConstraintComponent")
        }
        ResolvedConstraint::Node(_) => {
            concat!("http://www.w3.org/ns/shacl#", "NodeConstraintComponent")
        }
        ResolvedConstraint::HasValue(_) => {
            concat!("http://www.w3.org/ns/shacl#", "HasValueConstraintComponent")
        }
        ResolvedConstraint::In(_) => {
            concat!("http://www.w3.org/ns/shacl#", "InConstraintComponent")
        }
        ResolvedConstraint::QualifiedValueShape {
            min_count: Some(_), ..
        } => concat!(
            "http://www.w3.org/ns/shacl#",
            "QualifiedMinCountConstraintComponent"
        ),
        ResolvedConstraint::QualifiedValueShape { .. } => concat!(
            "http://www.w3.org/ns/shacl#",
            "QualifiedMaxCountConstraintComponent"
        ),
        ResolvedConstraint::Closed { .. } => {
            concat!("http://www.w3.org/ns/shacl#", "ClosedConstraintComponent")
        }
    }
}

fn severity_term(severity: &SeverityPlan) -> EncodedTerm {
    let iri = match severity {
        SeverityPlan::Trace => "Trace",
        SeverityPlan::Debug => "Debug",
        SeverityPlan::Info => "Info",
        SeverityPlan::Warning => "Warning",
        SeverityPlan::Violation => "Violation",
        SeverityPlan::Custom(term) => return term.clone(),
    };
    EncodedTerm(format!("<{SH}{iri}>"))
}

pub(crate) fn path_label(path: &PathPlan) -> String {
    match path {
        PathPlan::Predicate(predicate) => predicate.0.clone(),
        PathPlan::Alternative(paths) => format!(
            "({})",
            paths.iter().map(path_label).collect::<Vec<_>>().join(" | ")
        ),
        PathPlan::Sequence(paths) => format!(
            "({})",
            paths.iter().map(path_label).collect::<Vec<_>>().join(" / ")
        ),
        PathPlan::Inverse(path) => format!("^{}", path_label(path)),
        PathPlan::ZeroOrMore(path) => format!("{}*", path_label(path)),
        PathPlan::OneOrMore(path) => format!("{}+", path_label(path)),
        PathPlan::ZeroOrOne(path) => format!("{}?", path_label(path)),
    }
}
