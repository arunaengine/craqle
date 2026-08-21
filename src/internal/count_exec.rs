use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use crate::count_plan::CountValueDomain;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::sparql::{Result, SparqlError};
use crate::store::{QueryTermId, TermId};

const CANCELLATION_CHECK_INTERVAL: usize = 1_024;
#[cfg(not(test))]
const PARALLEL_COUNT_MIN_ROWS: u64 = 65_536;
#[cfg(test)]
const PARALLEL_COUNT_MIN_ROWS: u64 = 32;
type GraphOrphanCache = HashMap<QueryTermId, Option<Rc<HashSet<TermId>>>>;
type ParallelGraphOrphanCache = Arc<HashMap<QueryTermId, Option<Arc<HashSet<TermId>>>>>;

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
    if matches!(domain, CountValueDomain::Scalar)
        && matches!(selector, GraphSelector::DefaultUnion)
        && let Some(count) = exact_default_union_count(view, context, pattern)?
    {
        return Ok(Some(count));
    }

    let parallel_rows = if matches!(selector, GraphSelector::DefaultUnion)
        && matches!(domain, CountValueDomain::Scalar)
    {
        raw_row_estimate(view, context, pattern)?
    } else {
        None
    };
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
                let (matches, extracted) = cursor.matches(key);
                context.record_key_fields_extracted(extracted);
                if !matches {
                    continue;
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
        GraphSelector::DefaultUnion => {
            if parallel_rows.is_some_and(|rows| rows >= PARALLEL_COUNT_MIN_ROWS) {
                let workers = crate::query_worker::worker_count();
                match cursor.into_scalar_partitions(workers) {
                    Ok(partitions) => {
                        return parallel_default_union_count(view, context, partitions);
                    }
                    Err(original) => cursor = *original,
                }
            }
            default_union_count(view, context, &mut cursor, domain)
        }
        GraphSelector::Union => Ok(None),
    }
}

fn exact_default_union_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    pattern: QuadPattern,
) -> Result<Option<ScalarCount>> {
    if pattern.subject.is_some()
        || (pattern.predicate.is_none() && pattern.object.is_some())
        || view.qv_union_duplicate_free(context)? != Some(true)
    {
        return Ok(None);
    }

    let mut count = ScalarCount::default();
    for graph in view.graph_term_id_iter() {
        context.check_cancelled()?;
        let graph = graph?;
        if !view.graph_is_visible(context, graph)? {
            continue;
        }
        if !view.orphaned_ids(context, graph)?.is_empty() {
            return Ok(None);
        }
        let graph_count = match (pattern.predicate, pattern.object) {
            (Some(predicate), Some(object)) => {
                view.qv_gpo_count(context, graph, predicate, object)?
            }
            (Some(predicate), None) => view.qv_gp_count(context, graph, predicate)?,
            (None, None) => view.qv_g_count(context, graph)?,
            (None, Some(_)) => unreachable!("object-only patterns returned above"),
        };
        let Some(graph_count) = graph_count else {
            return Ok(None);
        };
        count.add(graph_count)?;
    }
    context.record_matching_quads(count.get());
    Ok(Some(count))
}

fn raw_row_estimate(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    pattern: QuadPattern,
) -> Result<Option<u64>> {
    if pattern.subject.is_some() {
        return Ok(None);
    }
    match (pattern.predicate, pattern.object) {
        (Some(predicate), Some(object)) => Ok(view.qv_po_count(context, predicate, object)?),
        (Some(predicate), None) => Ok(view.qv_p_count(context, predicate)?),
        (None, None) => Ok(view.qv_total_count(context)?),
        (None, Some(_)) => Ok(None),
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

pub(crate) fn object_subject_join_count(
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
        |object| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            table.observe(object)
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
        |subject| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            count.add(table.multiplicity(subject))
        },
    )?
    .is_none()
    {
        return Ok(None);
    }
    Ok(Some((count, intermediate_rows)))
}

pub(crate) fn subject_object_join_count(
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
        |subject| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            table.observe(subject)
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
        |object| {
            intermediate_rows = intermediate_rows.saturating_add(1);
            count.add(table.multiplicity(object))
        },
    )?
    .is_none()
    {
        return Ok(None);
    }
    Ok(Some((count, intermediate_rows)))
}

pub(crate) fn subject_star_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    patterns: &[(GraphSelector, QuadPattern)],
) -> Result<Option<(ScalarCount, u64)>> {
    let mut relations = Vec::with_capacity(patterns.len());
    let mut intermediate_rows = 0_u64;
    for &(selector, pattern) in patterns {
        let mut relation = SubjectKeySet::default();
        if for_each_join_key(
            view,
            context,
            selector,
            pattern,
            JoinKeyDomain::Subject,
            |subject| {
                intermediate_rows = intermediate_rows.saturating_add(1);
                relation.observe(subject)
            },
        )?
        .is_none()
        {
            return Ok(None);
        }
        relations.push(relation);
    }

    let Some((base_index, base)) = relations
        .iter()
        .enumerate()
        .min_by_key(|(_, relation)| relation.multiplicities.len())
    else {
        unreachable!("subject-star plans contain at least two triples")
    };
    let mut count = ScalarCount::default();
    for (subject, multiplicity) in &base.multiplicities {
        let mut product = *multiplicity;
        for (index, relation) in relations.iter().enumerate() {
            if index != base_index {
                product = product
                    .checked_mul(relation.multiplicity(*subject))
                    .ok_or_else(|| SparqlError::Evaluation("COUNT overflow".to_owned()))?;
            }
        }
        count.add(product)?;
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
                let (matches, extracted) = cursor.matches(key);
                context.record_key_fields_extracted(extracted);
                if !matches {
                    continue;
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
            let mut graph_cache = GraphOrphanCache::new();
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
                let (matches, extracted) = cursor.matches(key);
                context.record_key_fields_extracted(extracted);
                if !matches {
                    continue;
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
                let Some(orphaned) =
                    graph_orphans(view, context, &cursor, &mut graph_cache, key.graph())?
                else {
                    continue;
                };
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
    let mut graph_cache = GraphOrphanCache::new();
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
        let (matches, extracted) = cursor.matches(key);
        context.record_key_fields_extracted(extracted);
        if !matches {
            continue;
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
        let Some(orphaned) = graph_orphans(view, context, cursor, &mut graph_cache, key.graph())?
        else {
            continue;
        };
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

#[derive(Default)]
struct ParallelCountWork {
    count: ScalarCount,
    qv_keys: u64,
    qv_bytes: u64,
    candidate_quads: u64,
    matching_quads: u64,
    duplicate_groups: u64,
    skipped_copies: u64,
    key_fields_extracted: u64,
}

fn parallel_default_union_count(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    partitions: Vec<crate::query_cursor::RawQueryIndexKeyCursor>,
) -> Result<Option<ScalarCount>> {
    let graph_cache = parallel_graph_cache(view, context)?;
    let cancellation = context.cancellation();
    let results = crate::query_worker::map_ordered(partitions, |cursor| {
        count_default_union_partition(cursor, &graph_cache, &cancellation)
    })?;

    let mut count = ScalarCount::default();
    for result in results {
        count.add(result.count.get())?;
        context.record_qv_reads(result.qv_keys, result.qv_bytes);
        context.record_candidate_quads(result.candidate_quads);
        context.record_matching_quads(result.matching_quads);
        context.record_duplicate_groups(result.duplicate_groups);
        context.record_skipped_copies(result.skipped_copies);
        context.record_key_fields_extracted(result.key_fields_extracted);
    }
    Ok(Some(count))
}

fn parallel_graph_cache(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
) -> Result<ParallelGraphOrphanCache> {
    let mut cache = HashMap::new();
    for graph in view.graph_term_id_iter() {
        context.check_cancelled()?;
        let graph = graph?;
        let Some(query_graph) = view.query_term_id(context, graph)? else {
            continue;
        };
        let orphaned = if view.graph_is_visible(context, graph)? {
            Some(Arc::new((*view.orphaned_ids(context, graph)?).clone()))
        } else {
            None
        };
        cache.insert(query_graph, orphaned);
    }
    Ok(Arc::new(cache))
}

fn count_default_union_partition(
    mut cursor: crate::query_cursor::RawQueryIndexKeyCursor,
    graph_cache: &ParallelGraphOrphanCache,
    cancellation: &crate::query_context::QueryCancellation,
) -> Result<ParallelCountWork> {
    let mut result = ParallelCountWork::default();
    let mut current_group = None;
    let mut group_emitted = false;
    let mut work = 0usize;
    while let Some(key) = cursor.next_key() {
        let key = key?;
        result.qv_keys = result.qv_keys.saturating_add(1);
        result.qv_bytes = result.qv_bytes.saturating_add(key.bytes_read);
        result.candidate_quads = result.candidate_quads.saturating_add(1);
        work += 1;
        if work == CANCELLATION_CHECK_INTERVAL {
            work = 0;
            if cancellation.is_cancelled() {
                return Err(SparqlError::Cancelled);
            }
        }
        let (matches, extracted) = cursor.matches(key);
        result.key_fields_extracted = result.key_fields_extracted.saturating_add(extracted);
        if !matches {
            continue;
        }

        result.key_fields_extracted = result.key_fields_extracted.saturating_add(3);
        let subject = key.subject();
        let object = key.object();
        let group = (subject, key.predicate(), object);
        if current_group != Some(group) {
            current_group = Some(group);
            group_emitted = false;
            result.duplicate_groups = result.duplicate_groups.saturating_add(1);
        } else if group_emitted {
            result.skipped_copies = result.skipped_copies.saturating_add(1);
            continue;
        }

        result.key_fields_extracted = result.key_fields_extracted.saturating_add(1);
        let orphaned = graph_cache.get(&key.graph()).ok_or_else(|| {
            crate::store::StoreError::InvalidQueryIndexEncoding {
                context: "qv2 graph mapping",
                message: "query index row references an unknown graph".to_owned(),
            }
        })?;
        let Some(orphaned) = orphaned else {
            continue;
        };
        if !orphaned.is_empty()
            && (orphaned.contains(&cursor.source_term(subject)?)
                || orphaned.contains(&cursor.source_term(object)?))
        {
            continue;
        }

        group_emitted = true;
        result.matching_quads = result.matching_quads.saturating_add(1);
        result.count.increment()?;
    }
    if cancellation.is_cancelled() {
        return Err(SparqlError::Cancelled);
    }
    Ok(result)
}

fn graph_orphans(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    cursor: &crate::query_cursor::RawQueryIndexKeyCursor,
    cache: &mut GraphOrphanCache,
    query_graph: QueryTermId,
) -> Result<Option<Rc<HashSet<TermId>>>> {
    if let Some(orphaned) = cache.get(&query_graph) {
        return Ok(orphaned.clone());
    }
    let graph = cursor.source_term(query_graph)?;
    let orphaned = if view.graph_is_visible(context, graph)? {
        Some(view.orphaned_ids(context, graph)?)
    } else {
        None
    };
    cache.insert(query_graph, orphaned.clone());
    Ok(orphaned)
}
