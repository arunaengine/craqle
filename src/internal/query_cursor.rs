use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use fjall::{Keyspace, Readable, Snapshot};

use crate::query_context::ReadContext;
use crate::rdf_read::QuadPattern;
use crate::store::{
    EncodedQuad, GraphStore, QueryIndexCursorOrder, Result, StoreReadSnapshot, TermId,
};
use crate::validation_delta::DeltaQuadCursor;

enum SourceIterator {
    Single(Option<RawQuadCandidate>),
    Durable {
        snapshot: Snapshot,
        keyspace: Keyspace,
        iterator: fjall::Iter,
    },
    QueryIndex {
        _snapshot: Snapshot,
        iterator: fjall::Iter,
        order: QueryIndexCursorOrder,
    },
    PredicateObject {
        subjects: Arc<Vec<TermId>>,
        next: usize,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    },
    Object {
        entries: Arc<BTreeSet<(TermId, TermId)>>,
        last: Option<(TermId, TermId)>,
        graph: TermId,
        object: TermId,
    },
    Empty,
}

/// One candidate read from a durable or immutable in-memory index snapshot.
pub(crate) struct RawQuadCandidate {
    pub(crate) quad: EncodedQuad,
    pub(crate) live: bool,
    pub(crate) storage: CandidateStorage,
    pub(crate) bytes_read: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum CandidateStorage {
    Source,
    QueryIndex,
    Delta,
}

/// Lazy source cursor. It owns either a Fjall snapshot or a copy-on-write
/// in-memory range snapshot, but never an in-memory-index lock.
pub(crate) struct RawQuadCursor {
    source: SourceIterator,
}

impl RawQuadCursor {
    pub(crate) fn single(candidate: Option<RawQuadCandidate>) -> Self {
        Self {
            source: SourceIterator::Single(candidate),
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
                snapshot,
                keyspace: quads.clone(),
                iterator,
            },
        }
    }

    pub(crate) fn query_index(
        snapshot: Snapshot,
        keyspace: &Keyspace,
        order: QueryIndexCursorOrder,
        prefix: Vec<u8>,
    ) -> Self {
        let iterator = if prefix.is_empty() {
            snapshot.iter(keyspace)
        } else {
            snapshot.prefix(keyspace, prefix)
        };
        Self {
            source: SourceIterator::QueryIndex {
                _snapshot: snapshot,
                iterator,
                order,
            },
        }
    }

    pub(crate) fn predicate_object(
        subjects: Arc<Vec<TermId>>,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Self {
        Self {
            source: SourceIterator::PredicateObject {
                subjects,
                next: 0,
                graph,
                predicate,
                object,
            },
        }
    }

    pub(crate) fn object(
        entries: Arc<BTreeSet<(TermId, TermId)>>,
        graph: TermId,
        object: TermId,
    ) -> Self {
        Self {
            source: SourceIterator::Object {
                entries,
                last: None,
                graph,
                object,
            },
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            source: SourceIterator::Empty,
        }
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
                    live: GraphStore::quad_value_is_live(value.as_ref()),
                    storage: CandidateStorage::Source,
                    bytes_read: (key.len() + value.len()) as u64,
                }))
            }
            SourceIterator::QueryIndex {
                iterator, order, ..
            } => {
                let guard = iterator.next()?;
                let (key, value) = match guard.into_inner() {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error.into())),
                };
                if !value.as_ref().is_empty() {
                    return Some(Err(crate::store::StoreError::InvalidEncoding {
                        context: "qv1 query index value",
                        message: format!("expected empty value, found {} bytes", value.len()),
                    }));
                }
                let quad = match GraphStore::decode_query_index_key(*order, key.as_ref()) {
                    Ok(quad) => quad,
                    Err(error) => return Some(Err(error)),
                };
                Some(Ok(RawQuadCandidate {
                    quad,
                    live: true,
                    storage: CandidateStorage::QueryIndex,
                    bytes_read: (key.len() + value.len()) as u64,
                }))
            }
            SourceIterator::PredicateObject {
                subjects,
                next,
                graph,
                predicate,
                object,
            } => {
                let subject = *subjects.get(*next)?;
                *next += 1;
                Some(Ok(RawQuadCandidate {
                    quad: EncodedQuad {
                        graph: *graph,
                        subject,
                        predicate: *predicate,
                        object: *object,
                    },
                    live: true,
                    storage: CandidateStorage::Source,
                    bytes_read: 64,
                }))
            }
            SourceIterator::Object {
                entries,
                last,
                graph,
                object,
            } => {
                let next = match *last {
                    Some(last) => entries.range((Excluded(last), Unbounded)).next(),
                    None => entries.iter().next(),
                };
                let &(subject, predicate) = next?;
                *last = Some((subject, predicate));
                Some(Ok(RawQuadCandidate {
                    quad: EncodedQuad {
                        graph: *graph,
                        subject,
                        predicate,
                        object: *object,
                    },
                    live: true,
                    storage: CandidateStorage::Source,
                    bytes_read: 64,
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
        Self {
            store,
            snapshot,
            context,
            source: Some(QuerySource::Raw(raw)),
            pattern,
            candidates_since_check: 0,
            finished: false,
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
        }
    }

    fn fail(&mut self, error: crate::store::StoreError) -> Option<Result<EncodedQuad>> {
        self.finished = true;
        Some(Err(error))
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
        }
    }

    pub(crate) fn default_union(
        store: &'store GraphStore,
        snapshot: &'store StoreReadSnapshot,
        context: &'context ReadContext<'visibility>,
        raw: RawQuadCursor,
        pattern: QuadPattern,
    ) -> Self {
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
        live: GraphStore::quad_value_is_live(value.as_ref()),
        storage: CandidateStorage::Source,
        bytes_read: (64 + value.len()) as u64,
    }))
}
