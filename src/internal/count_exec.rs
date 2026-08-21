use std::collections::HashMap;

use crate::count_plan::CountValueDomain;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::sparql::{Result, SparqlError};
use crate::store::{QueryTermId, TermId};

const CANCELLATION_CHECK_INTERVAL: usize = 1_024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScalarCount(u64);

impl ScalarCount {
    pub(crate) fn get(self) -> u64 {
        self.0
    }

    fn increment(&mut self) -> Result<()> {
        self.add(1)
    }

    fn add(&mut self, value: u64) -> Result<()> {
        self.0 = self
            .0
            .checked_add(value)
            .ok_or_else(|| SparqlError::Evaluation("COUNT overflow".to_owned()))?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SubjectCountStream {
    last: Option<QueryTermId>,
    count: ScalarCount,
}

#[derive(Default)]
pub(crate) struct ObjectCountStream {
    last: Option<QueryTermId>,
    count: ScalarCount,
}

#[derive(Default)]
pub(crate) struct SubjectKeySet {
    multiplicities: HashMap<QueryTermId, u64>,
}

impl SubjectKeySet {
    fn observe(&mut self, subject: QueryTermId) -> Result<()> {
        let count = self.multiplicities.entry(subject).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| SparqlError::Evaluation("COUNT overflow".to_owned()))?;
        Ok(())
    }

    fn multiplicity(&self, subject: QueryTermId) -> u64 {
        self.multiplicities.get(&subject).copied().unwrap_or(0)
    }
}

#[derive(Default)]
pub(crate) struct ObjectKeySet {
    multiplicities: HashMap<QueryTermId, u64>,
}

impl ObjectKeySet {
    fn observe(&mut self, object: QueryTermId) -> Result<()> {
        let count = self.multiplicities.entry(object).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| SparqlError::Evaluation("COUNT overflow".to_owned()))?;
        Ok(())
    }

    fn multiplicity(&self, object: QueryTermId) -> u64 {
        self.multiplicities.get(&object).copied().unwrap_or(0)
    }
}

impl ObjectCountStream {
    fn observe(&mut self, object: QueryTermId) -> Result<()> {
        if self.last != Some(object) {
            self.last = Some(object);
            self.count.increment()?;
        }
        Ok(())
    }

    fn finish(self) -> ScalarCount {
        self.count
    }
}

impl SubjectCountStream {
    fn observe(&mut self, subject: QueryTermId) -> Result<()> {
        if self.last != Some(subject) {
            self.last = Some(subject);
            self.count.increment()?;
        }
        Ok(())
    }

    fn finish(self) -> ScalarCount {
        self.count
    }
}

pub(crate) fn single_pattern_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    selector: GraphSelector,
    pattern: QuadPattern,
    domain: CountValueDomain,
) -> Result<Option<ScalarCount>> {
    if matches!(domain, CountValueDomain::Scalar)
        && let GraphSelector::Named(graph) = selector
        && let Some(count) = exact_named_count(view, context, graph, pattern)?
    {
        return Ok(Some(count));
    }

    let Some(mut cursor) = view.raw_query_index_keys(context, selector, pattern)? else {
        return Ok(None);
    };
    match selector {
        GraphSelector::Named(graph) => {
            let orphaned = view.orphaned_ids(context, graph)?;
            let mut count = ScalarCount::default();
            let mut subjects = SubjectCountStream::default();
            let mut objects = ObjectCountStream::default();
            let mut work = 0usize;
            while let Some(key) = cursor.next_key() {
                let key = key?;
                context.increment_candidate_quads();
                context.record_qv_read(key.bytes_read);
                work += 1;
                if work == CANCELLATION_CHECK_INTERVAL {
                    work = 0;
                    context.check_cancelled()?;
                }

                let subject = (matches!(domain, CountValueDomain::Subject) || !orphaned.is_empty())
                    .then(|| {
                        context.record_key_fields_extracted(1);
                        key.subject()
                    });
                let object = (matches!(domain, CountValueDomain::Object) || !orphaned.is_empty())
                    .then(|| {
                        context.record_key_fields_extracted(1);
                        key.object()
                    });
                if !orphaned.is_empty() {
                    let subject = cursor.source_term(subject.expect("subject was extracted"))?;
                    let object = cursor.source_term(object.expect("object was extracted"))?;
                    if orphaned.contains(&subject) || orphaned.contains(&object) {
                        continue;
                    }
                }
                context.increment_matching_quads();
                match domain {
                    CountValueDomain::Scalar => count.increment()?,
                    CountValueDomain::Subject => {
                        subjects.observe(subject.expect("subject domain extracted subject"))?
                    }
                    CountValueDomain::Object => {
                        objects.observe(object.expect("object domain extracted object"))?
                    }
                }
            }
            Ok(Some(match domain {
                CountValueDomain::Scalar => count,
                CountValueDomain::Subject => subjects.finish(),
                CountValueDomain::Object => objects.finish(),
            }))
        }
        GraphSelector::DefaultUnion => default_union_count(view, context, &mut cursor, domain),
        GraphSelector::Union => Ok(None),
    }
}

pub(crate) fn subject_join_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    build_selector: GraphSelector,
    build_pattern: QuadPattern,
    probe_selector: GraphSelector,
    probe_pattern: QuadPattern,
) -> Result<Option<(ScalarCount, u64)>> {
    let mut table = SubjectKeySet::default();
    let mut intermediate_rows = 0_u64;
    if for_each_join_key(
        view,
        context,
        build_selector,
        build_pattern,
        JoinKeyDomain::Subject,
        |key| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            table.observe(key)
        },
    )?
    .is_none()
    {
        return Ok(None);
    }

    let mut count = ScalarCount::default();
    if for_each_join_key(
        view,
        context,
        probe_selector,
        probe_pattern,
        JoinKeyDomain::Subject,
        |key| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            count.add(table.multiplicity(key))
        },
    )?
    .is_none()
    {
        return Ok(None);
    }
    Ok(Some((count, intermediate_rows)))
}

pub(crate) fn object_join_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    build_selector: GraphSelector,
    build_pattern: QuadPattern,
    probe_selector: GraphSelector,
    probe_pattern: QuadPattern,
) -> Result<Option<(ScalarCount, u64)>> {
    let mut table = ObjectKeySet::default();
    let mut intermediate_rows = 0_u64;
    if for_each_join_key(
        view,
        context,
        build_selector,
        build_pattern,
        JoinKeyDomain::Object,
        |key| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            table.observe(key)
        },
    )?
    .is_none()
    {
        return Ok(None);
    }

    let mut count = ScalarCount::default();
    if for_each_join_key(
        view,
        context,
        probe_selector,
        probe_pattern,
        JoinKeyDomain::Object,
        |key| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            count.add(table.multiplicity(key))
        },
    )?
    .is_none()
    {
        return Ok(None);
    }
    Ok(Some((count, intermediate_rows)))
}

#[derive(Clone, Copy)]
enum JoinKeyDomain {
    Subject,
    Object,
}

fn for_each_join_key(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    selector: GraphSelector,
    pattern: QuadPattern,
    domain: JoinKeyDomain,
    mut observe: impl FnMut(QueryTermId) -> Result<()>,
) -> Result<Option<()>> {
    let Some(mut cursor) = view.raw_query_index_keys(context, selector, pattern)? else {
        return Ok(None);
    };
    match selector {
        GraphSelector::Named(graph) => {
            let orphaned = view.orphaned_ids(context, graph)?;
            let mut work = 0usize;
            while let Some(key) = cursor.next_key() {
                let key = key?;
                context.increment_candidate_quads();
                context.record_qv_read(key.bytes_read);
                work += 1;
                if work == CANCELLATION_CHECK_INTERVAL {
                    work = 0;
                    context.check_cancelled()?;
                }

                context.record_key_fields_extracted(1);
                let selected = match domain {
                    JoinKeyDomain::Subject => key.subject(),
                    JoinKeyDomain::Object => key.object(),
                };
                if !orphaned.is_empty() {
                    let subject = match domain {
                        JoinKeyDomain::Subject => selected,
                        JoinKeyDomain::Object => {
                            context.record_key_fields_extracted(1);
                            key.subject()
                        }
                    };
                    let object = match domain {
                        JoinKeyDomain::Subject => {
                            context.record_key_fields_extracted(1);
                            key.object()
                        }
                        JoinKeyDomain::Object => selected,
                    };
                    if orphaned.contains(&cursor.source_term(subject)?)
                        || orphaned.contains(&cursor.source_term(object)?)
                    {
                        continue;
                    }
                }
                context.increment_matching_quads();
                observe(selected)?;
            }
        }
        GraphSelector::DefaultUnion => {
            let mut current_group = None;
            let mut group_emitted = false;
            let mut work = 0usize;
            while let Some(key) = cursor.next_key() {
                let key = key?;
                context.increment_candidate_quads();
                context.record_qv_read(key.bytes_read);
                work += 1;
                if work == CANCELLATION_CHECK_INTERVAL {
                    work = 0;
                    context.check_cancelled()?;
                }

                context.record_key_fields_extracted(3);
                let subject = key.subject();
                let object = key.object();
                let group = (subject, key.predicate(), object);
                if current_group != Some(group) {
                    current_group = Some(group);
                    group_emitted = false;
                    context.increment_duplicate_groups();
                } else if group_emitted {
                    context.increment_skipped_copies();
                    continue;
                }

                context.record_key_fields_extracted(1);
                let graph = cursor.source_term(key.graph())?;
                if !view.graph_is_visible(context, graph)? {
                    continue;
                }
                let orphaned = view.orphaned_ids(context, graph)?;
                if !orphaned.is_empty()
                    && (orphaned.contains(&cursor.source_term(subject)?)
                        || orphaned.contains(&cursor.source_term(object)?))
                {
                    continue;
                }

                group_emitted = true;
                context.increment_matching_quads();
                observe(match domain {
                    JoinKeyDomain::Subject => subject,
                    JoinKeyDomain::Object => object,
                })?;
            }
        }
        GraphSelector::Union => return Ok(None),
    }
    Ok(Some(()))
}

fn exact_named_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    graph: TermId,
    pattern: QuadPattern,
) -> Result<Option<ScalarCount>> {
    if !view.graph_is_visible(context, graph)? {
        return Ok(Some(ScalarCount(0)));
    }
    if !view.orphaned_ids(context, graph)?.is_empty() {
        return Ok(None);
    }
    if pattern.subject.is_some() || (pattern.predicate.is_none() && pattern.object.is_some()) {
        return Ok(None);
    }
    let count = match (pattern.predicate, pattern.object) {
        (Some(predicate), Some(object)) => view.qv_gpo_count(context, graph, predicate, object)?,
        (Some(predicate), None) => view.qv_gp_count(context, graph, predicate)?,
        (None, None) => view.qv_g_count(context, graph)?,
        (None, Some(_)) => unreachable!("object-only patterns returned above"),
    };
    if let Some(count) = count {
        context.record_matching_quads(count);
    }
    Ok(count.map(ScalarCount))
}

fn default_union_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    cursor: &mut crate::query_cursor::RawQueryIndexKeyCursor,
    domain: CountValueDomain,
) -> Result<Option<ScalarCount>> {
    let mut count = ScalarCount::default();
    let mut subjects = SubjectCountStream::default();
    let mut objects = ObjectCountStream::default();
    let mut current_group = None;
    let mut group_emitted = false;
    let mut work = 0usize;
    while let Some(key) = cursor.next_key() {
        let key = key?;
        context.increment_candidate_quads();
        context.record_qv_read(key.bytes_read);
        work += 1;
        if work == CANCELLATION_CHECK_INTERVAL {
            work = 0;
            context.check_cancelled()?;
        }

        let mut subject = None;
        let mut object = None;
        let group = match domain {
            CountValueDomain::Scalar => {
                context.record_key_fields_extracted(3);
                let extracted_subject = key.subject();
                let extracted_object = key.object();
                subject = Some(extracted_subject);
                object = Some(extracted_object);
                (extracted_subject, key.predicate(), extracted_object)
            }
            CountValueDomain::Subject => {
                context.record_key_fields_extracted(1);
                let extracted = key.subject();
                subject = Some(extracted);
                (extracted, QueryTermId(0), QueryTermId(0))
            }
            CountValueDomain::Object => {
                context.record_key_fields_extracted(1);
                let extracted = key.object();
                object = Some(extracted);
                (extracted, QueryTermId(0), QueryTermId(0))
            }
        };
        if current_group != Some(group) {
            current_group = Some(group);
            group_emitted = false;
            context.increment_duplicate_groups();
        } else if group_emitted {
            context.increment_skipped_copies();
            continue;
        }

        context.record_key_fields_extracted(1);
        let graph = cursor.source_term(key.graph())?;
        if !view.graph_is_visible(context, graph)? {
            continue;
        }
        let orphaned = view.orphaned_ids(context, graph)?;
        if !orphaned.is_empty() {
            let subject = match subject {
                Some(subject) => subject,
                None => {
                    context.record_key_fields_extracted(1);
                    key.subject()
                }
            };
            let object = match object {
                Some(object) => object,
                None => {
                    context.record_key_fields_extracted(1);
                    key.object()
                }
            };
            let subject = cursor.source_term(subject)?;
            let object = cursor.source_term(object)?;
            if orphaned.contains(&subject) || orphaned.contains(&object) {
                continue;
            }
        }

        group_emitted = true;
        context.increment_matching_quads();
        match domain {
            CountValueDomain::Scalar => count.increment()?,
            CountValueDomain::Subject => {
                subjects.observe(subject.expect("subject domain extracted subject"))?
            }
            CountValueDomain::Object => {
                objects.observe(object.expect("object domain extracted object"))?
            }
        }
    }
    Ok(Some(match domain {
        CountValueDomain::Scalar => count,
        CountValueDomain::Subject => subjects.finish(),
        CountValueDomain::Object => objects.finish(),
    }))
}
