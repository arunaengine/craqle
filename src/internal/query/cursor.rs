//! Scans snapshot query indexes with bounded key cursors.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Bound::{Excluded, Included};
use std::rc::Rc;
use std::sync::PoisonError;

use fjall::{Keyspace, Readable, Snapshot};

use crate::query::context::{QueryCost, ReadContext};
use crate::rdf_read::{GraphVisibilityInput, QuadPattern, graph_orphans};
use crate::store::{
    EncodedQuad, GraphStore, IndexCursorOrder, QueryTermId, Result, SourceCache, StoreReadSnapshot,
    TermId,
};
use crate::validation_delta::DeltaQuadCursor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CountGrouping {
    Subject,
    Object,
    None,
}

fn grouping_for_order(
    order: IndexCursorOrder,
    mut fixed: impl FnMut(usize) -> bool,
) -> CountGrouping {
    let columns = match order {
        IndexCursorOrder::Gspo => [0, 1, 2, 3],
        IndexCursorOrder::Gpos => [0, 2, 3, 1],
        IndexCursorOrder::Spog => [1, 2, 3, 0],
        IndexCursorOrder::Posg => [2, 3, 1, 0],
        IndexCursorOrder::Ospg => [3, 1, 2, 0],
        IndexCursorOrder::Gosp => [0, 3, 1, 2],
    };
    columns
        .into_iter()
        .find(|column| !fixed(*column))
        .map_or(CountGrouping::None, |column| match column {
            1 => CountGrouping::Subject,
            3 => CountGrouping::Object,
            _ => CountGrouping::None,
        })
}

enum SourceIterator {
    Single(Option<RawQuadCandidate>),
    Durable {
        _snapshot: Snapshot,
        _keyspace: Keyspace,
        iterator: fjall::Iter,
    },
    QueryIndex {
        snapshot: Snapshot,
        query_to_term: Keyspace,
        iterator: fjall::Iter,
        order: IndexCursorOrder,
    },
    Empty,
}

/// One candidate read from a durable or immutable in-memory index snapshot.
pub(crate) struct RawQuadCandidate {
    pub(crate) quad: EncodedQuad,
    pub(crate) live: bool,
    pub(crate) storage: CandidateStorage,
    pub(crate) bytes_read: u64,
    pub(crate) key_fields_extracted: u8,
    pub(crate) encoded_quad_constructed: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum CandidateStorage {
    Source,
    QueryIndex,
    Delta,
}

#[derive(Clone, Copy)]
pub(crate) struct RawIndexKey {
    bytes: [u8; 32],
    order: IndexCursorOrder,
    pub(crate) bytes_read: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct RawIndexPattern {
    graph: Option<QueryTermId>,
    subject: Option<QueryTermId>,
    predicate: Option<QueryTermId>,
    object: Option<QueryTermId>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DenseSpace {
    generation: u64,
    snapshot: u64,
    scope: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DenseTerm {
    query: QueryTermId,
    scope: u64,
}

impl PartialEq for DenseTerm {
    fn eq(&self, other: &Self) -> bool {
        self.query == other.query && self.scope == other.scope
    }
}

impl Eq for DenseTerm {}

impl Hash for DenseTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.query.hash(state);
        self.scope.hash(state);
    }
}

#[derive(Clone)]
pub(crate) struct DenseResolver {
    inner: Rc<DenseResolverInner>,
}

struct DenseResolverInner {
    snapshot: Snapshot,
    query_to_term: Keyspace,
    costs: QueryCost,
    space: DenseSpace,
    sources: RefCell<crate::cache::BoundedCache<QueryTermId, TermId>>,
    shared: Option<SourceCache>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DenseQuad {
    pub(crate) graph: DenseTerm,
    pub(crate) graph_source: TermId,
    pub(crate) subject: DenseTerm,
    subject_source: TermId,
    pub(crate) predicate: DenseTerm,
    pub(crate) object: DenseTerm,
    object_source: TermId,
    source_mask: u8,
}

impl DenseQuad {
    pub(crate) fn subject_source(&self) -> Option<TermId> {
        (self.source_mask & 1 != 0).then_some(self.subject_source)
    }

    pub(crate) fn object_source(&self) -> Option<TermId> {
        (self.source_mask & 2 != 0).then_some(self.object_source)
    }
}

pub(crate) struct IndexScan<'a> {
    pub(crate) order: IndexCursorOrder,
    pub(crate) pattern: QuadPattern,
    pub(crate) query_id_limit: Option<u64>,
    pub(crate) costs: &'a QueryCost,
}

pub(crate) struct RawIndexScan<'a> {
    pub(crate) keyspace: &'a Keyspace,
    pub(crate) query_to_term: &'a Keyspace,
    pub(crate) order: IndexCursorOrder,
    pub(crate) prefix: Vec<u8>,
    pub(crate) pattern: RawIndexPattern,
    pub(crate) query_id_limit: u64,
}

pub(crate) struct QueryIndexScan<'a> {
    pub(crate) keyspace: &'a Keyspace,
    pub(crate) query_to_term: &'a Keyspace,
    pub(crate) order: IndexCursorOrder,
    pub(crate) prefix: Vec<u8>,
}

impl RawIndexPattern {
    pub(crate) fn from_terms(terms: [Option<QueryTermId>; 4]) -> Self {
        let [graph, subject, predicate, object] = terms;
        Self {
            graph,
            subject,
            predicate,
            object,
        }
    }

    pub(crate) fn new(
        graph: Option<QueryTermId>,
        subject: Option<QueryTermId>,
        predicate: Option<QueryTermId>,
        object: Option<QueryTermId>,
    ) -> Self {
        Self {
            graph,
            subject,
            predicate,
            object,
        }
    }

    pub(crate) fn without_prefix(mut self, order: IndexCursorOrder, prefix_terms: usize) -> Self {
        let columns = match order {
            IndexCursorOrder::Gspo => [0, 1, 2, 3],
            IndexCursorOrder::Gpos => [0, 2, 3, 1],
            IndexCursorOrder::Spog => [1, 2, 3, 0],
            IndexCursorOrder::Posg => [2, 3, 1, 0],
            IndexCursorOrder::Ospg => [3, 1, 2, 0],
            IndexCursorOrder::Gosp => [0, 3, 1, 2],
        };
        for column in columns.into_iter().take(prefix_terms) {
            match column {
                0 => self.graph = None,
                1 => self.subject = None,
                2 => self.predicate = None,
                3 => self.object = None,
                _ => unreachable!("query-index columns are four terms"),
            }
        }
        self
    }
}

impl RawIndexKey {
    fn term_at(self, index: usize) -> QueryTermId {
        QueryTermId(u64::from_be_bytes(
            self.bytes[index * 8..(index + 1) * 8]
                .try_into()
                .expect("query-index term slices are eight bytes"),
        ))
    }

    pub(crate) fn graph(self) -> QueryTermId {
        self.term_at(match self.order {
            IndexCursorOrder::Gspo | IndexCursorOrder::Gpos | IndexCursorOrder::Gosp => 0,
            IndexCursorOrder::Spog | IndexCursorOrder::Posg | IndexCursorOrder::Ospg => 3,
        })
    }

    pub(crate) fn subject(self) -> QueryTermId {
        self.term_at(match self.order {
            IndexCursorOrder::Gspo | IndexCursorOrder::Ospg => 1,
            IndexCursorOrder::Gpos => 3,
            IndexCursorOrder::Spog => 0,
            IndexCursorOrder::Posg | IndexCursorOrder::Gosp => 2,
        })
    }

    pub(crate) fn predicate(self) -> QueryTermId {
        self.term_at(match self.order {
            IndexCursorOrder::Gspo | IndexCursorOrder::Ospg => 2,
            IndexCursorOrder::Gpos | IndexCursorOrder::Spog => 1,
            IndexCursorOrder::Posg => 0,
            IndexCursorOrder::Gosp => 3,
        })
    }

    pub(crate) fn object(self) -> QueryTermId {
        self.term_at(match self.order {
            IndexCursorOrder::Gspo => 3,
            IndexCursorOrder::Gpos | IndexCursorOrder::Spog => 2,
            IndexCursorOrder::Posg | IndexCursorOrder::Gosp => 1,
            IndexCursorOrder::Ospg => 0,
        })
    }
}

pub(crate) struct RawIndexCursor {
    snapshot: Snapshot,
    keyspace: Keyspace,
    query_to_term: Keyspace,
    iterator: KeyRead,
    order: IndexCursorOrder,
    prefix: Vec<u8>,
    pattern: RawIndexPattern,
    query_id_limit: u64,
    count_grouping: CountGrouping,
    costs: QueryCost,
}

/// Index keys opened on first read; a fully bound prefix is one point read.
enum KeyRead {
    Unopened,
    Range(fjall::Iter),
    Merged(Vec<(RawIndexCursor, Option<RawIndexKey>)>),
    Done,
}

impl RawIndexCursor {
    pub(crate) fn new(snapshot: Snapshot, scan: RawIndexScan<'_>) -> Self {
        let iterator = KeyRead::Unopened;
        let prefix_terms = scan.prefix.len() / 8;
        let count_grouping = grouping_for_order(scan.order, |column| {
            let column_position = match scan.order {
                IndexCursorOrder::Gspo => [0, 1, 2, 3],
                IndexCursorOrder::Gpos => [0, 2, 3, 1],
                IndexCursorOrder::Spog => [1, 2, 3, 0],
                IndexCursorOrder::Posg => [2, 3, 1, 0],
                IndexCursorOrder::Ospg => [3, 1, 2, 0],
                IndexCursorOrder::Gosp => [0, 3, 1, 2],
            }
            .iter()
            .position(|candidate| *candidate == column)
            .expect("query-index columns contain every term");
            column_position < prefix_terms
                || match column {
                    0 => scan.pattern.graph.is_some(),
                    1 => scan.pattern.subject.is_some(),
                    2 => scan.pattern.predicate.is_some(),
                    3 => scan.pattern.object.is_some(),
                    _ => unreachable!("query-index columns are four terms"),
                }
        });
        Self {
            snapshot,
            keyspace: scan.keyspace.clone(),
            query_to_term: scan.query_to_term.clone(),
            iterator,
            order: scan.order,
            prefix: scan.prefix,
            pattern: scan.pattern,
            query_id_limit: scan.query_id_limit,
            count_grouping,
            costs: QueryCost::default(),
        }
    }

    pub(crate) fn count_grouping(&self) -> CountGrouping {
        self.count_grouping
    }

    pub(crate) fn into_scalar_partitions(
        self,
        count: usize,
    ) -> std::result::Result<Vec<Self>, Box<Self>> {
        let prefix_terms = self.prefix.len() / 8;
        let columns = match self.order {
            IndexCursorOrder::Gspo => [0, 1, 2, 3],
            IndexCursorOrder::Gpos => [0, 2, 3, 1],
            IndexCursorOrder::Spog => [1, 2, 3, 0],
            IndexCursorOrder::Posg => [2, 3, 1, 0],
            IndexCursorOrder::Ospg => [3, 1, 2, 0],
            IndexCursorOrder::Gosp => [0, 3, 1, 2],
        };
        if count < 2
            || prefix_terms >= columns.len()
            || columns[prefix_terms] == 0
            || self.query_id_limit < 2
        {
            return Err(Box::new(self));
        }

        let Self {
            snapshot,
            keyspace,
            query_to_term,
            iterator: _,
            order,
            prefix,
            pattern,
            query_id_limit,
            count_grouping,
            costs,
        } = self;
        let count = count.min(usize::try_from(query_id_limit).unwrap_or(count));
        let width = query_id_limit.div_ceil(count as u64);
        Ok((0..count)
            .filter_map(|partition| {
                let start = (partition as u64).saturating_mul(width);
                if start >= query_id_limit {
                    return None;
                }
                let end = ((partition + 1) as u64)
                    .saturating_mul(width)
                    .min(query_id_limit);
                let mut lower = prefix.clone();
                lower.extend_from_slice(&start.to_be_bytes());
                let mut upper = prefix.clone();
                upper.extend_from_slice(&end.to_be_bytes());
                let iterator =
                    KeyRead::Range(snapshot.range(&keyspace, (Included(lower), Excluded(upper))));
                Some(Self {
                    snapshot: snapshot.clone(),
                    keyspace: keyspace.clone(),
                    query_to_term: query_to_term.clone(),
                    iterator,
                    order,
                    prefix: prefix.clone(),
                    pattern,
                    query_id_limit,
                    count_grouping,
                    costs: costs.clone(),
                })
            })
            .collect())
    }

    pub(crate) fn next_key(&mut self) -> Option<Result<RawIndexKey>> {
        if let KeyRead::Merged(ranges) = &mut self.iterator {
            for (cursor, head) in ranges.iter_mut() {
                if head.is_none() {
                    match cursor.next_key() {
                        Some(Ok(key)) => *head = Some(key),
                        Some(Err(error)) => return Some(Err(error)),
                        None => {}
                    }
                }
            }
            // Graph-prefixed ranges share their remaining column order, keeping copies adjacent.
            let next = ranges
                .iter()
                .enumerate()
                .filter_map(|(index, (_, head))| head.map(|key| (index, key)))
                .min_by(|(_, left), (_, right)| left.bytes[8..].cmp(&right.bytes[8..]))?
                .0;
            return ranges[next].1.take().map(Ok);
        }
        if matches!(self.iterator, KeyRead::Unopened) {
            if self.prefix.len() == 32 {
                self.iterator = KeyRead::Done;
                return match self.snapshot.get(&self.keyspace, &self.prefix) {
                    Ok(Some(value)) => Some(self.index_key(&self.prefix, &value)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error.into())),
                };
            }
            self.iterator = KeyRead::Range(if self.prefix.is_empty() {
                self.snapshot.iter(&self.keyspace)
            } else {
                self.snapshot.prefix(&self.keyspace, &self.prefix)
            });
        }
        let KeyRead::Range(iterator) = &mut self.iterator else {
            return None;
        };
        let (key, value) = match iterator.next()?.into_inner() {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error.into())),
        };
        Some(self.index_key(&key, &value))
    }

    fn index_key(&self, key: &[u8], value: &[u8]) -> Result<RawIndexKey> {
        if !value.is_empty() {
            return Err(crate::store::StoreError::InvalidIndexEncoding {
                context: "qv2 query index value",
                message: format!("expected empty value, found {} bytes", value.len()),
            });
        }
        let bytes = match <[u8; 32]>::try_from(key) {
            Ok(bytes) => bytes,
            Err(_) => {
                return Err(crate::store::StoreError::InvalidIndexEncoding {
                    context: "qv2 query index key",
                    message: format!("expected 32 bytes, found {}", key.len()),
                });
            }
        };
        Ok(RawIndexKey {
            bytes,
            order: self.order,
            bytes_read: (key.len() + value.len()) as u64,
        })
    }

    pub(crate) fn track_costs(mut self, costs: QueryCost) -> Self {
        self.costs = costs;
        self
    }

    pub(crate) fn merge_graphs(
        mut self,
        pattern: RawIndexPattern,
        graphs: &HashSet<QueryTermId>,
    ) -> Self {
        self.iterator = KeyRead::Merged(
            graphs
                .iter()
                .map(|graph| {
                    let cursor = Self::new(
                        self.snapshot.clone(),
                        RawIndexScan {
                            keyspace: &self.keyspace,
                            query_to_term: &self.query_to_term,
                            order: self.order,
                            prefix: Vec::new(),
                            pattern,
                            query_id_limit: self.query_id_limit,
                        },
                    )
                    .narrow(RawIndexPattern {
                        graph: Some(*graph),
                        ..pattern
                    });
                    (cursor, None)
                })
                .collect(),
        );
        self.pattern = pattern;
        self
    }

    pub(crate) fn narrow(mut self, pattern: RawIndexPattern) -> Self {
        let terms = match self.order {
            IndexCursorOrder::Gspo => [
                pattern.graph,
                pattern.subject,
                pattern.predicate,
                pattern.object,
            ],
            IndexCursorOrder::Gpos => [
                pattern.graph,
                pattern.predicate,
                pattern.object,
                pattern.subject,
            ],
            IndexCursorOrder::Spog => [
                pattern.subject,
                pattern.predicate,
                pattern.object,
                pattern.graph,
            ],
            IndexCursorOrder::Posg => [
                pattern.predicate,
                pattern.object,
                pattern.subject,
                pattern.graph,
            ],
            IndexCursorOrder::Ospg => [
                pattern.object,
                pattern.subject,
                pattern.predicate,
                pattern.graph,
            ],
            IndexCursorOrder::Gosp => [
                pattern.graph,
                pattern.object,
                pattern.subject,
                pattern.predicate,
            ],
        };
        let mut prefix = Vec::new();
        for term in terms.into_iter().map_while(|term| term) {
            prefix.extend_from_slice(&term.0.to_be_bytes());
        }
        self.iterator = KeyRead::Unopened;
        self.prefix = prefix;
        self.pattern = pattern.without_prefix(self.order, self.prefix.len() / 8);
        self
    }

    pub(crate) fn resolver(&self, generation: u64, request: DenseRequest) -> DenseResolver {
        DenseResolver {
            inner: Rc::new(DenseResolverInner {
                snapshot: self.snapshot.clone(),
                query_to_term: self.query_to_term.clone(),
                costs: self.costs.clone(),
                space: DenseSpace {
                    generation,
                    snapshot: self.snapshot.seqno(),
                    scope: request.scope,
                },
                sources: RefCell::new(crate::cache::BoundedCache::new(
                    request.cache_entries,
                    request.cache_bytes,
                )),
                shared: request.shared,
            }),
        }
    }

    pub(crate) fn source_term(&self, term: QueryTermId) -> Result<TermId> {
        let (term, bytes) =
            GraphStore::decode_query_term(&self.snapshot, &self.query_to_term, term)?;
        self.costs.reverse_mapping(bytes);
        Ok(term)
    }

    pub(crate) fn matches(&self, key: RawIndexKey) -> (bool, u64) {
        let mut extracted = 0_u64;
        for (expected, actual) in [
            (
                self.pattern.graph,
                RawIndexKey::graph as fn(RawIndexKey) -> QueryTermId,
            ),
            (self.pattern.subject, RawIndexKey::subject),
            (self.pattern.predicate, RawIndexKey::predicate),
            (self.pattern.object, RawIndexKey::object),
        ] {
            if let Some(expected) = expected {
                extracted += 1;
                if expected != actual(key) {
                    return (false, extracted);
                }
            }
        }
        (true, extracted)
    }
}

impl DenseTerm {
    pub(crate) fn new(query: QueryTermId, scope: u64) -> Self {
        Self { query, scope }
    }

    pub(crate) fn query(self) -> QueryTermId {
        self.query
    }

    pub(crate) fn scope(self) -> u64 {
        self.scope
    }
}

impl DenseResolver {
    pub(crate) fn space(&self) -> DenseSpace {
        self.inner.space
    }

    pub(crate) fn scope(&self) -> u64 {
        self.inner.space.scope
    }

    pub(crate) fn source(&self, term: DenseTerm) -> Result<TermId> {
        if term.scope != self.inner.space.scope {
            return Err(crate::store::StoreError::IndexVerificationFailed(
                "dense-term-scope-mismatch",
            ));
        }
        if let Some(source) = self.inner.sources.borrow_mut().get_cloned(&term.query) {
            return Ok(source);
        }
        let key = (self.inner.space.generation, term.query);
        let shared = self.inner.shared.as_ref().and_then(|shared| {
            shared
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .peek(&key)
                .copied()
        });
        if let Some(source) = shared {
            self.inner
                .sources
                .borrow_mut()
                .insert(term.query, source, 0);
            return Ok(source);
        }
        let (source, bytes) = GraphStore::decode_query_term(
            &self.inner.snapshot,
            &self.inner.query_to_term,
            term.query,
        )?;
        self.inner.costs.reverse_mapping(bytes);
        if let Some(shared) = &self.inner.shared {
            shared
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key, source, 0);
        }
        self.inner
            .sources
            .borrow_mut()
            .insert(term.query, source, 0);
        Ok(source)
    }
}

#[derive(Clone)]
pub(crate) struct DenseRequest {
    pub(crate) scope: u64,
    pub(crate) cache_entries: usize,
    pub(crate) cache_bytes: usize,
    pub(crate) shared: Option<SourceCache>,
}

pub(crate) struct DenseInput<'store, 'context, 'visibility> {
    pub(crate) store: &'store GraphStore,
    pub(crate) snapshot: &'store StoreReadSnapshot,
    pub(crate) context: &'context ReadContext<'visibility>,
    pub(crate) raw: RawIndexCursor,
    pub(crate) generation: u64,
    pub(crate) scope: u64,
    pub(crate) resolver: Option<DenseResolver>,
    pub(crate) cache_entries: usize,
    pub(crate) cache_bytes: usize,
    pub(crate) default_union: bool,
    pub(crate) source_hints: [Option<(QueryTermId, TermId)>; 4],
    /// Keys outside these graphs are skipped before any visibility work.
    pub(crate) graphs: Option<Rc<HashSet<QueryTermId>>>,
}

pub(crate) struct DenseCursor<'store, 'context, 'visibility> {
    store: &'store GraphStore,
    snapshot: &'store StoreReadSnapshot,
    context: &'context ReadContext<'visibility>,
    raw: RawIndexCursor,
    resolver: DenseResolver,
    last_graph: Option<(QueryTermId, TermId)>,
    /// The last graph's visibility and orphans; both are fixed within one request.
    last_orphans: Option<(QueryTermId, Option<Rc<HashSet<TermId>>>)>,
    current_group: Option<(QueryTermId, QueryTermId, QueryTermId)>,
    group_emitted: bool,
    default_union: bool,
    source_hints: [Option<(QueryTermId, TermId)>; 4],
    graphs: Option<Rc<HashSet<QueryTermId>>>,
    candidates_since_check: usize,
    finished: bool,
}

impl<'store, 'context, 'visibility> DenseCursor<'store, 'context, 'visibility> {
    pub(crate) fn new(input: DenseInput<'store, 'context, 'visibility>) -> Self {
        let space = DenseSpace {
            generation: input.generation,
            snapshot: input.raw.snapshot.seqno(),
            scope: input.scope,
        };
        let resolver = match input.resolver {
            Some(resolver) if resolver.space() == space => resolver,
            _ => input.raw.resolver(
                input.generation,
                DenseRequest {
                    scope: input.scope,
                    cache_entries: input.cache_entries,
                    cache_bytes: input.cache_bytes,
                    shared: Some(input.store.source_cache()),
                },
            ),
        };
        Self {
            store: input.store,
            snapshot: input.snapshot,
            context: input.context,
            raw: input.raw,
            resolver,
            last_graph: None,
            last_orphans: None,
            current_group: None,
            group_emitted: false,
            default_union: input.default_union,
            source_hints: input.source_hints,
            graphs: input.graphs,
            candidates_since_check: 0,
            finished: false,
        }
    }

    pub(crate) fn resolver(&self) -> DenseResolver {
        self.resolver.clone()
    }

    fn fail(&mut self, error: crate::store::StoreError) -> Option<Result<DenseQuad>> {
        self.finished = true;
        Some(Err(error))
    }

    fn source_hint(&self, index: usize, query: QueryTermId) -> Option<TermId> {
        self.source_hints[index]
            .filter(|(hint, _)| *hint == query)
            .map(|(_, source)| source)
    }

    fn graph_source(&mut self, graph: QueryTermId) -> Result<TermId> {
        let source = if let Some(source) = self.source_hint(0, graph) {
            source
        } else if let Some((cached, source)) = self.last_graph {
            if cached == graph {
                source
            } else {
                self.resolver
                    .source(DenseTerm::new(graph, self.resolver.space().scope))?
            }
        } else {
            self.resolver
                .source(DenseTerm::new(graph, self.resolver.space().scope))?
        };
        self.last_graph = Some((graph, source));
        Ok(source)
    }

    fn resolve_source(&self, query: QueryTermId) -> Result<TermId> {
        self.resolver
            .source(DenseTerm::new(query, self.resolver.space().scope))
    }

    fn account(&mut self, bytes: u64, fields: u64) -> Result<()> {
        self.context.increment_candidate_quads();
        self.context.record_qv_read(bytes);
        self.context.record_key_fields(fields);
        self.candidates_since_check += 1;
        if self.candidates_since_check == CANCELLATION_CHECK_INTERVAL {
            self.candidates_since_check = 0;
            self.context.check_cancelled()?;
        }
        Ok(())
    }
}

impl Iterator for DenseCursor<'_, '_, '_> {
    type Item = Result<DenseQuad>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Err(error) = self.context.check_cancelled() {
            return self.fail(error);
        }
        loop {
            let key = match self.raw.next_key()? {
                Ok(key) => key,
                Err(error) => return self.fail(error),
            };
            let (matches, _) = self.raw.matches(key);
            let query = [key.graph(), key.subject(), key.predicate(), key.object()];
            if let Err(error) = self.account(key.bytes_read, 4) {
                return self.fail(error);
            }
            if query.iter().any(|term| term.0 >= self.raw.query_id_limit) {
                return self.fail(crate::store::StoreError::InvalidIndexEncoding {
                    context: "qv2 query index key",
                    message: "query ID exceeds the admitted generation bound".to_owned(),
                });
            }
            if !matches
                || self
                    .graphs
                    .as_ref()
                    .is_some_and(|graphs| !graphs.contains(&query[0]))
            {
                continue;
            }
            let graph_source = match self.graph_source(query[0]) {
                Ok(source) => source,
                Err(error) => return self.fail(error),
            };
            let orphaned = match &self.last_orphans {
                Some((graph, orphaned)) if *graph == query[0] => orphaned.clone(),
                _ => {
                    let orphaned = match graph_orphans(GraphVisibilityInput {
                        store: self.store,
                        snapshot: self.snapshot,
                        context: self.context,
                        graph: graph_source,
                    }) {
                        Ok(orphaned) => orphaned,
                        Err(error) => return self.fail(error),
                    };
                    self.last_orphans = Some((query[0], orphaned.clone()));
                    orphaned
                }
            };
            let Some(orphaned) = orphaned else {
                continue;
            };
            let mut subject_source = self.source_hint(1, query[1]);
            let mut object_source = self.source_hint(3, query[3]);
            if !orphaned.is_empty() {
                if subject_source.is_none() {
                    subject_source = Some(if query[1] == query[0] {
                        graph_source
                    } else {
                        match self.resolve_source(query[1]) {
                            Ok(source) => source,
                            Err(error) => return self.fail(error),
                        }
                    });
                }
                if object_source.is_none() {
                    object_source = Some(if query[3] == query[0] {
                        graph_source
                    } else if query[3] == query[1] {
                        subject_source.expect("subject was resolved for orphan filtering")
                    } else {
                        match self.resolve_source(query[3]) {
                            Ok(source) => source,
                            Err(error) => return self.fail(error),
                        }
                    });
                }
                if orphaned.contains(&subject_source.expect("subject source is present"))
                    || orphaned.contains(&object_source.expect("object source is present"))
                {
                    continue;
                }
            }
            if self.context.validation_graph().is_none() {
                self.context.increment_orphan_checks();
            }
            if self.default_union {
                let group = (query[1], query[2], query[3]);
                if self.current_group != Some(group) {
                    self.current_group = Some(group);
                    self.group_emitted = false;
                    self.context.increment_duplicate_groups();
                } else {
                    self.context.record_skipped_copies(1);
                }
                if self.group_emitted {
                    continue;
                }
                self.group_emitted = true;
            }
            self.context.increment_matching_quads();
            let scope = self.resolver.space().scope;
            let source_mask =
                u8::from(subject_source.is_some()) | (u8::from(object_source.is_some()) << 1);
            return Some(Ok(DenseQuad {
                graph: DenseTerm::new(query[0], scope),
                graph_source,
                subject: DenseTerm::new(query[1], scope),
                subject_source: subject_source.unwrap_or(TermId(0)),
                predicate: DenseTerm::new(query[2], scope),
                object: DenseTerm::new(query[3], scope),
                object_source: object_source.unwrap_or(TermId(0)),
                source_mask,
            }));
        }
    }
}

/// Owns a durable snapshot or a single candidate without holding index locks.
pub(crate) struct RawQuadCursor {
    source: SourceIterator,
    costs: QueryCost,
}

impl RawQuadCursor {
    fn count_grouping(&self, pattern: QuadPattern) -> CountGrouping {
        let fixed = |column| match column {
            0 => pattern.graph.is_some(),
            1 => pattern.subject.is_some(),
            2 => pattern.predicate.is_some(),
            3 => pattern.object.is_some(),
            _ => unreachable!("quad columns are four terms"),
        };
        match &self.source {
            SourceIterator::Durable { .. } => grouping_for_order(IndexCursorOrder::Gspo, fixed),
            SourceIterator::QueryIndex { order, .. } => grouping_for_order(*order, fixed),
            SourceIterator::Single(_) | SourceIterator::Empty => CountGrouping::None,
        }
    }

    pub(crate) fn single(candidate: Option<RawQuadCandidate>) -> Self {
        Self {
            source: SourceIterator::Single(candidate),
            costs: QueryCost::default(),
        }
    }

    pub(crate) fn new(snapshot: Snapshot, quads: &Keyspace, pattern: QuadPattern) -> Self {
        let iterator = match (pattern.graph, pattern.subject, pattern.predicate) {
            (Some(graph), Some(subject), Some(predicate)) => {
                let mut prefix = [0u8; 48];
                prefix[..16].copy_from_slice(&graph.to_be_bytes());
                prefix[16..32].copy_from_slice(&subject.to_be_bytes());
                prefix[32..].copy_from_slice(&predicate.to_be_bytes());
                snapshot.prefix(quads, prefix)
            }
            (Some(graph), Some(subject), None) => {
                let mut prefix = [0u8; 32];
                prefix[..16].copy_from_slice(&graph.to_be_bytes());
                prefix[16..].copy_from_slice(&subject.to_be_bytes());
                snapshot.prefix(quads, prefix)
            }
            (Some(graph), None, _) => snapshot.prefix(quads, graph.to_be_bytes()),
            (None, _, _) => snapshot.iter(quads),
        };
        Self {
            source: SourceIterator::Durable {
                _snapshot: snapshot,
                _keyspace: quads.clone(),
                iterator,
            },
            costs: QueryCost::default(),
        }
    }

    pub(crate) fn query_index(snapshot: Snapshot, scan: QueryIndexScan<'_>) -> Self {
        let iterator = if scan.prefix.is_empty() {
            snapshot.iter(scan.keyspace)
        } else {
            snapshot.prefix(scan.keyspace, scan.prefix)
        };
        Self {
            source: SourceIterator::QueryIndex {
                snapshot,
                query_to_term: scan.query_to_term.clone(),
                iterator,
                order: scan.order,
            },
            costs: QueryCost::default(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            source: SourceIterator::Empty,
            costs: QueryCost::default(),
        }
    }

    pub(crate) fn track_costs(mut self, costs: QueryCost) -> Self {
        self.costs = costs;
        self
    }

    pub(crate) fn next_candidate(&mut self) -> Option<Result<RawQuadCandidate>> {
        match &mut self.source {
            SourceIterator::Single(candidate) => candidate.take().map(Ok),
            SourceIterator::Durable { iterator, .. } => {
                let guard = iterator.next()?;
                let (key, value) = match guard.into_inner() {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error.into())),
                };
                let quad = match GraphStore::decode_quad_key(key.as_ref()) {
                    Ok(quad) => quad,
                    Err(error) => return Some(Err(error)),
                };
                Some(Ok(RawQuadCandidate {
                    quad,
                    live: GraphStore::quad_is_live(value.as_ref()),
                    storage: CandidateStorage::Source,
                    bytes_read: (key.len() + value.len()) as u64,
                    key_fields_extracted: 4,
                    encoded_quad_constructed: true,
                }))
            }
            SourceIterator::QueryIndex {
                snapshot,
                query_to_term,
                iterator,
                order,
            } => {
                let guard = iterator.next()?;
                let (key, value) = match guard.into_inner() {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error.into())),
                };
                if !value.as_ref().is_empty() {
                    return Some(Err(crate::store::StoreError::InvalidIndexEncoding {
                        context: "qv2 query index value",
                        message: format!("expected empty value, found {} bytes", value.len()),
                    }));
                }
                let query_quad = match GraphStore::decode_query_key(*order, key.as_ref()) {
                    Ok(quad) => quad,
                    Err(error) => return Some(Err(error)),
                };
                let decode = |term| {
                    let (term, bytes) =
                        GraphStore::decode_query_term(snapshot, query_to_term, term)?;
                    self.costs.reverse_mapping(bytes);
                    Ok::<TermId, crate::store::StoreError>(term)
                };
                let quad = match (|| {
                    Ok(EncodedQuad {
                        graph: decode(query_quad.graph)?,
                        subject: decode(query_quad.subject)?,
                        predicate: decode(query_quad.predicate)?,
                        object: decode(query_quad.object)?,
                    })
                })() {
                    Ok(quad) => quad,
                    Err(error) => return Some(Err(error)),
                };
                Some(Ok(RawQuadCandidate {
                    quad,
                    live: true,
                    storage: CandidateStorage::QueryIndex,
                    bytes_read: (key.len() + value.len()) as u64,
                    key_fields_extracted: 4,
                    encoded_quad_constructed: true,
                }))
            }
            SourceIterator::Empty => None,
        }
    }
}

const CANCELLATION_CHECK_INTERVAL: usize = 1_024;

/// Lazy filtered RDF cursor. Fixed-term, graph-visibility, and orphan checks
/// happen as each durable source candidate is consumed.
pub(crate) struct QueryCursor<'store, 'context, 'visibility> {
    store: &'store GraphStore,
    snapshot: &'store StoreReadSnapshot,
    context: &'context ReadContext<'visibility>,
    source: Option<QuerySource<'store>>,
    pattern: QuadPattern,
    candidates_since_check: usize,
    finished: bool,
    count_grouping: CountGrouping,
}

enum QuerySource<'store> {
    Raw(RawQuadCursor),
    DefaultUnion {
        raw: RawQuadCursor,
        current_group: Option<(TermId, TermId, TermId)>,
        group_emitted: bool,
    },
    Delta(DeltaQuadCursor<'store>),
}

impl<'store, 'context, 'visibility> QueryCursor<'store, 'context, 'visibility> {
    pub(crate) fn new(
        store: &'store GraphStore,
        snapshot: &'store StoreReadSnapshot,
        context: &'context ReadContext<'visibility>,
        raw: RawQuadCursor,
        pattern: QuadPattern,
    ) -> Self {
        let count_grouping = raw.count_grouping(pattern);
        Self {
            store,
            snapshot,
            context,
            source: Some(QuerySource::Raw(raw)),
            pattern,
            candidates_since_check: 0,
            finished: false,
            count_grouping,
        }
    }

    pub(crate) fn empty(
        store: &'store GraphStore,
        snapshot: &'store StoreReadSnapshot,
        context: &'context ReadContext<'visibility>,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            snapshot,
            context,
            source: None,
            pattern,
            candidates_since_check: 0,
            finished: false,
            count_grouping: CountGrouping::None,
        }
    }

    fn fail(&mut self, error: crate::store::StoreError) -> Option<Result<EncodedQuad>> {
        self.finished = true;
        Some(Err(error))
    }

    pub(crate) fn count_grouping(&self) -> CountGrouping {
        self.count_grouping
    }

    pub(crate) fn delta(
        store: &'store GraphStore,
        snapshot: &'store StoreReadSnapshot,
        context: &'context ReadContext<'visibility>,
        delta: DeltaQuadCursor<'store>,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            snapshot,
            context,
            source: Some(QuerySource::Delta(delta)),
            pattern,
            candidates_since_check: 0,
            finished: false,
            count_grouping: CountGrouping::None,
        }
    }

    pub(crate) fn default_union(
        store: &'store GraphStore,
        snapshot: &'store StoreReadSnapshot,
        context: &'context ReadContext<'visibility>,
        raw: RawQuadCursor,
        pattern: QuadPattern,
    ) -> Self {
        let count_grouping = raw.count_grouping(pattern);
        Self {
            store,
            snapshot,
            context,
            source: Some(QuerySource::DefaultUnion {
                raw,
                current_group: None,
                group_emitted: false,
            }),
            pattern,
            candidates_since_check: 0,
            finished: false,
            count_grouping,
        }
    }

    fn next_source_candidate(&mut self) -> Option<Result<RawQuadCandidate>> {
        let source = self.source.as_mut()?;
        match source {
            QuerySource::Raw(raw) => raw.next_candidate(),
            QuerySource::Delta(delta) => delta.next_candidate(),
            QuerySource::DefaultUnion { .. } => {
                unreachable!("default union has its own constant-state cursor")
            }
        }
    }

    fn account_candidate(&mut self, candidate: &RawQuadCandidate) -> Result<()> {
        self.context.increment_candidate_quads();
        self.context
            .record_key_fields(u64::from(candidate.key_fields_extracted));
        if candidate.encoded_quad_constructed {
            self.context.increment_quad_builds();
        }
        match candidate.storage {
            CandidateStorage::Source => self.context.record_source_read(candidate.bytes_read),
            CandidateStorage::QueryIndex => self.context.record_qv_read(candidate.bytes_read),
            CandidateStorage::Delta => {}
        }
        self.candidates_since_check += 1;
        if self.candidates_since_check == CANCELLATION_CHECK_INTERVAL {
            self.candidates_since_check = 0;
            self.context.check_cancelled()?;
        }
        Ok(())
    }

    fn next_default_union(&mut self) -> Option<Result<EncodedQuad>> {
        loop {
            let next = match self.source.as_mut() {
                Some(QuerySource::DefaultUnion { raw, .. }) => raw.next_candidate(),
                _ => unreachable!("default-union cursor lost its selected source"),
            };
            let candidate = match next {
                Some(Ok(candidate)) => candidate,
                Some(Err(error)) => return self.fail(error),
                None => {
                    self.finished = true;
                    return None;
                }
            };
            if let Err(error) = self.account_candidate(&candidate) {
                return self.fail(error);
            }
            if !candidate.live || !self.pattern.matches(candidate.quad) {
                continue;
            }

            let group_already_emitted = match self.source.as_mut() {
                Some(QuerySource::DefaultUnion {
                    current_group,
                    group_emitted,
                    ..
                }) => {
                    let group = (
                        candidate.quad.subject,
                        candidate.quad.predicate,
                        candidate.quad.object,
                    );
                    if *current_group != Some(group) {
                        *current_group = Some(group);
                        *group_emitted = false;
                        self.context.increment_duplicate_groups();
                    } else {
                        self.context.increment_skipped_copies();
                    }
                    *group_emitted
                }
                _ => unreachable!("default-union cursor lost its selected source"),
            };
            if group_already_emitted {
                continue;
            }
            let visible = match crate::rdf_read::quad_is_visible(
                self.store,
                self.snapshot,
                self.context,
                candidate.quad,
            ) {
                Ok(visible) => visible,
                Err(error) => return self.fail(error),
            };
            if !visible {
                continue;
            }
            let Some(QuerySource::DefaultUnion { group_emitted, .. }) = self.source.as_mut() else {
                unreachable!("default-union cursor lost its selected source");
            };
            *group_emitted = true;
            self.context.increment_matching_quads();
            return Some(Ok(candidate.quad));
        }
    }
}

impl Iterator for QueryCursor<'_, '_, '_> {
    type Item = Result<EncodedQuad>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Err(error) = self.context.check_cancelled() {
            return self.fail(error);
        }

        if matches!(self.source, Some(QuerySource::DefaultUnion { .. })) {
            return self.next_default_union();
        }

        loop {
            if self.source.is_none() {
                self.finished = true;
                return None;
            };
            let next = self.next_source_candidate();
            let candidate = match next {
                Some(Ok(candidate)) => candidate,
                Some(Err(error)) => {
                    return self.fail(error);
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };
            if let Err(error) = self.account_candidate(&candidate) {
                return self.fail(error);
            }

            if !candidate.live || !self.pattern.matches(candidate.quad) {
                continue;
            }
            match crate::rdf_read::quad_is_visible(
                self.store,
                self.snapshot,
                self.context,
                candidate.quad,
            ) {
                Ok(true) => {
                    self.context.increment_matching_quads();
                    return Some(Ok(candidate.quad));
                }
                Ok(false) => continue,
                Err(error) => return self.fail(error),
            }
        }
    }
}

pub(crate) fn point_candidate(
    snapshot: &Snapshot,
    quads: &Keyspace,
    quad: EncodedQuad,
) -> Result<Option<RawQuadCandidate>> {
    let Some(value) = snapshot.get(
        quads,
        GraphStore::quad_key(quad.graph, quad.subject, quad.predicate, quad.object),
    )?
    else {
        return Ok(None);
    };
    Ok(Some(RawQuadCandidate {
        quad,
        live: GraphStore::quad_is_live(value.as_ref()),
        storage: CandidateStorage::Source,
        bytes_read: (64 + value.len()) as u64,
        key_fields_extracted: 0,
        encoded_quad_constructed: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_cover_orders() {
        let graph = QueryTermId(1);
        let subject = QueryTermId(2);
        let predicate = QueryTermId(3);
        let object = QueryTermId(4);
        for (order, terms) in [
            (IndexCursorOrder::Gspo, [graph, subject, predicate, object]),
            (IndexCursorOrder::Gpos, [graph, predicate, object, subject]),
            (IndexCursorOrder::Spog, [subject, predicate, object, graph]),
            (IndexCursorOrder::Posg, [predicate, object, subject, graph]),
            (IndexCursorOrder::Ospg, [object, subject, predicate, graph]),
            (IndexCursorOrder::Gosp, [graph, object, subject, predicate]),
        ] {
            let mut bytes = [0_u8; 32];
            for (index, term) in terms.into_iter().enumerate() {
                bytes[index * 8..(index + 1) * 8].copy_from_slice(&term.0.to_be_bytes());
            }
            let key = RawIndexKey {
                bytes,
                order,
                bytes_read: 32,
            };
            assert_eq!(key.graph(), graph);
            assert_eq!(key.subject(), subject);
            assert_eq!(key.predicate(), predicate);
            assert_eq!(key.object(), object);
        }
    }
}
