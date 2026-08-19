use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::rc::Rc;
use std::sync::Arc;

use fjall::{Keyspace, Readable, Snapshot};

use crate::query_context::ReadContext;
use crate::rdf_read::QuadPattern;
use crate::store::{EncodedQuad, GraphStore, Result, TermId};
use crate::validation_delta::DeltaQuadCursor;

enum SourceIterator {
    Durable {
        _snapshot: Snapshot,
        iterator: fjall::Iter,
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
}

/// Lazy source cursor. It owns either a Fjall snapshot or a copy-on-write
/// in-memory range snapshot, but never an in-memory-index lock.
pub(crate) struct RawQuadCursor {
    source: SourceIterator,
}

impl RawQuadCursor {
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
                iterator,
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
    context: &'context ReadContext<'visibility>,
    source: Option<QuerySource<'store>>,
    pattern: QuadPattern,
    candidates_since_check: usize,
    finished: bool,
}

enum QuerySource<'store> {
    Raw(RawQuadCursor),
    Graphs {
        graphs: Rc<Vec<TermId>>,
        next_graph: usize,
        current: Option<RawQuadCursor>,
        pattern: QuadPattern,
    },
    Delta(DeltaQuadCursor<'store>),
}

impl<'store, 'context, 'visibility> QueryCursor<'store, 'context, 'visibility> {
    pub(crate) fn new(
        store: &'store GraphStore,
        context: &'context ReadContext<'visibility>,
        raw: RawQuadCursor,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            context,
            source: Some(QuerySource::Raw(raw)),
            pattern,
            candidates_since_check: 0,
            finished: false,
        }
    }

    pub(crate) fn empty(
        store: &'store GraphStore,
        context: &'context ReadContext<'visibility>,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            context,
            source: None,
            pattern,
            candidates_since_check: 0,
            finished: false,
        }
    }

    pub(crate) fn graphs(
        store: &'store GraphStore,
        context: &'context ReadContext<'visibility>,
        graphs: Rc<Vec<TermId>>,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            context,
            source: Some(QuerySource::Graphs {
                graphs,
                next_graph: 0,
                current: None,
                pattern,
            }),
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
        context: &'context ReadContext<'visibility>,
        delta: DeltaQuadCursor<'store>,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            store,
            context,
            source: Some(QuerySource::Delta(delta)),
            pattern,
            candidates_since_check: 0,
            finished: false,
        }
    }

    fn next_source_candidate(&mut self) -> Option<Result<RawQuadCandidate>> {
        let store = self.store;
        let context = self.context;
        let source = self.source.as_mut()?;
        match source {
            QuerySource::Raw(raw) => raw.next_candidate(),
            QuerySource::Delta(delta) => delta.next_candidate(),
            QuerySource::Graphs {
                graphs,
                next_graph,
                current,
                pattern,
            } => loop {
                if let Some(cursor) = current {
                    if let Some(candidate) = cursor.next_candidate() {
                        return Some(candidate);
                    }
                    *current = None;
                }

                let graph = *graphs.get(*next_graph)?;
                *next_graph += 1;
                if let Err(error) = context.check_cancelled() {
                    return Some(Err(error));
                }
                match crate::rdf_read::graph_is_visible(store, context, graph) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => return Some(Err(error)),
                }
                context.increment_index_seeks();
                *current = Some(store.raw_quad_cursor(QuadPattern {
                    graph: Some(graph),
                    ..*pattern
                }));
            },
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
            self.context.increment_candidate_quads();
            self.candidates_since_check += 1;
            if self.candidates_since_check == CANCELLATION_CHECK_INTERVAL {
                self.candidates_since_check = 0;
                if let Err(error) = self.context.check_cancelled() {
                    return self.fail(error);
                }
            }

            if !candidate.live || !self.pattern.matches(candidate.quad) {
                continue;
            }
            match crate::rdf_read::quad_is_visible(self.store, self.context, candidate.quad) {
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
    }))
}
