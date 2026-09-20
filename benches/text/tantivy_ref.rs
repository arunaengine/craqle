//! Collects eligible Tantivy results with exhaustive and pruning controls.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{BinaryHeap, HashSet};

use tantivy::query::{
    Bm25StatisticsProvider as Bm25Stats, BooleanQuery, EnableScoring, Occur, Query, TermQuery,
};
use tantivy::{DocAddress, DocId, Searcher, Term};

use super::engine::{
    EngineError, HitIdentity, QueryBounds, Result as EngineResult, SearchHit, SearchReport,
    WorkCount, next_down,
};

pub struct TantivyRequest<'a> {
    pub searcher: &'a Searcher,
    pub query: &'a dyn Query,
    pub statistics: &'a dyn Bm25Stats,
    pub candidate: &'a dyn Fn(DocAddress) -> EngineResult<Option<[u8; 32]>>,
    pub identity: &'a dyn Fn(DocAddress) -> EngineResult<HitIdentity>,
    pub generation: u64,
    pub limit: usize,
    pub bounds: QueryBounds,
}

#[derive(Clone, Copy, Debug)]
struct RankedDoc {
    score: f32,
    stable: [u8; 32],
    address: DocAddress,
}

struct RetainedDocs {
    docs: BinaryHeap<RankedDoc>,
    keys: HashSet<[u8; 32]>,
}

struct RetainInput {
    candidate: RankedDoc,
    limit: usize,
}

impl PartialEq for RankedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score).is_eq()
            && self.stable == other.stable
            && self.address == other.address
    }
}

impl Eq for RankedDoc {}

impl PartialOrd for RankedDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.stable.cmp(&other.stable))
            .then_with(|| self.address.cmp(&other.address))
    }
}

pub fn collect_pruned(request: TantivyRequest<'_>) -> EngineResult<SearchReport> {
    if request.limit == 0 || pruning_safe(&request)? {
        return collect(request, true);
    }
    let mut report = collect(request, false)?;
    report.work.pruning_fallback = true;
    Ok(report)
}

pub fn collect_full(request: TantivyRequest<'_>) -> EngineResult<SearchReport> {
    collect(request, false)
}

fn collect(request: TantivyRequest<'_>, pruning: bool) -> EngineResult<SearchReport> {
    if request.limit == 0 {
        return Ok(SearchReport {
            generation: request.generation,
            hits: Vec::new(),
            work: WorkCount::default(),
        });
    }
    check_limit(
        "tantivy retained docs",
        request.bounds.candidates,
        request.limit,
    )?;
    let rank_bytes = request
        .limit
        .saturating_mul(std::mem::size_of::<RankedDoc>().saturating_add(64));
    check_limit("tantivy retained bytes", request.bounds.bytes, rank_bytes)?;
    let weight = request
        .query
        .weight(EnableScoring::enabled_from_statistics_provider(
            request.statistics,
            request.searcher,
        ))?;
    let mut retained = RetainedDocs {
        docs: BinaryHeap::with_capacity(request.limit),
        keys: HashSet::with_capacity(request.limit),
    };
    let mut work = WorkCount::default();
    for (segment_ord, reader) in request.searcher.segment_readers().iter().enumerate() {
        let mut failed = None;
        let initial_threshold = threshold(&retained.docs, request.limit);
        let mut collect_doc = |doc: DocId, score: f32| {
            if failed.is_some() {
                return f32::INFINITY;
            }
            work.posting_rows = work.posting_rows.saturating_add(1);
            work.scored_docs = work.scored_docs.saturating_add(1);
            if let Err(error) = check_limit(
                "tantivy callbacks",
                request.bounds.postings,
                work.posting_rows,
            ) {
                failed = Some(error);
                return f32::INFINITY;
            }
            if reader
                .alive_bitset()
                .is_some_and(|alive| !alive.is_alive(doc))
            {
                return threshold(&retained.docs, request.limit);
            }
            let address = DocAddress::new(segment_ord as u32, doc);
            work.metadata_reads = work.metadata_reads.saturating_add(1);
            let stable = match (request.candidate)(address) {
                Ok(Some(stable)) => stable,
                Ok(None) => {
                    work.rejected_docs = work.rejected_docs.saturating_add(1);
                    return threshold(&retained.docs, request.limit);
                }
                Err(error) => {
                    failed = Some(error);
                    return f32::INFINITY;
                }
            };
            work.candidate_docs = work.candidate_docs.saturating_add(1);
            if let Err(error) = check_limit(
                "tantivy candidates",
                request.bounds.candidates,
                work.candidate_docs,
            ) {
                failed = Some(error);
                return f32::INFINITY;
            }
            work.bytes = work.bytes.saturating_add(stable.len());
            if let Err(error) =
                check_limit("tantivy result bytes", request.bounds.bytes, work.bytes)
            {
                failed = Some(error);
                return f32::INFINITY;
            }
            retain_doc(
                &mut retained,
                RetainInput {
                    candidate: RankedDoc {
                        score,
                        stable,
                        address,
                    },
                    limit: request.limit,
                },
                &mut work,
            );
            threshold(&retained.docs, request.limit)
        };
        if pruning {
            weight.for_each_pruning(initial_threshold, reader, &mut collect_doc)?;
        } else {
            weight.for_each(reader, &mut |doc, score| {
                let _ = collect_doc(doc, score);
            })?;
        }
        if let Some(error) = failed {
            return Err(error);
        }
    }

    let mut ranked = retained.docs.into_vec();
    ranked.sort_by(rank_order);
    let mut hits = Vec::with_capacity(ranked.len());
    for ranked in ranked {
        work.final_reads = work.final_reads.saturating_add(1);
        let identity = (request.identity)(ranked.address)?;
        work.bytes = work
            .bytes
            .saturating_add(identity.graph.len())
            .saturating_add(identity.subject.len());
        check_limit("tantivy result bytes", request.bounds.bytes, work.bytes)?;
        hits.push(SearchHit {
            graph: identity.graph,
            subject: identity.subject,
            score: ranked.score,
        });
    }
    Ok(SearchReport {
        generation: request.generation,
        hits,
        work,
    })
}

fn pruning_safe(request: &TantivyRequest<'_>) -> EngineResult<bool> {
    let [reader] = request.searcher.segment_readers() else {
        return Ok(false);
    };
    let Some(terms) = pruning_terms(request.query) else {
        return Ok(false);
    };
    let Some(field) = terms.first().map(|term| term.field()) else {
        return Ok(false);
    };
    if terms.iter().any(|term| term.field() != field)
        || request.statistics.total_num_docs()? != u64::from(reader.max_doc())
        || request.statistics.total_num_tokens(field)?
            != reader.inverted_index(field)?.total_num_tokens()
    {
        return Ok(false);
    }
    for term in terms {
        if request.statistics.doc_freq(term)? != request.searcher.doc_freq(term)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn pruning_terms(query: &dyn Query) -> Option<Vec<&Term>> {
    if let Some(term) = query.downcast_ref::<TermQuery>() {
        return Some(vec![term.term()]);
    }
    let boolean = query.downcast_ref::<BooleanQuery>()?;
    let mut terms = Vec::with_capacity(boolean.clauses().len());
    for (occur, query) in boolean.clauses() {
        if *occur != Occur::Should {
            return None;
        }
        terms.push(query.downcast_ref::<TermQuery>()?.term());
    }
    Some(terms)
}

fn retain_doc(retained: &mut RetainedDocs, input: RetainInput, work: &mut WorkCount) {
    let candidate = input.candidate;
    if retained.keys.contains(&candidate.stable) {
        work.deduplicated_docs = work.deduplicated_docs.saturating_add(1);
        if retained
            .docs
            .iter()
            .find(|current| current.stable == candidate.stable)
            .is_some_and(|current| candidate < *current)
        {
            let mut values = std::mem::take(&mut retained.docs).into_vec();
            if let Some(current) = values
                .iter_mut()
                .find(|current| current.stable == candidate.stable)
            {
                *current = candidate;
            }
            retained.docs = BinaryHeap::from(values);
        }
        return;
    }
    if retained.docs.len() < input.limit {
        retained.keys.insert(candidate.stable);
        retained.docs.push(candidate);
    } else if retained.docs.peek().is_some_and(|worst| candidate < *worst) {
        if let Some(removed) = retained.docs.pop() {
            retained.keys.remove(&removed.stable);
        }
        retained.keys.insert(candidate.stable);
        retained.docs.push(candidate);
    }
}

fn threshold(retained: &BinaryHeap<RankedDoc>, limit: usize) -> f32 {
    if retained.len() < limit {
        return f32::NEG_INFINITY;
    }
    retained
        .peek()
        .map_or(f32::NEG_INFINITY, |doc| next_down(doc.score))
}

fn rank_order(left: &RankedDoc, right: &RankedDoc) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.stable.cmp(&right.stable))
        .then_with(|| left.address.cmp(&right.address))
}

fn check_limit(resource: &'static str, limit: usize, actual: usize) -> EngineResult<()> {
    if actual > limit {
        return Err(EngineError::Limit {
            resource,
            limit,
            actual,
        });
    }
    Ok(())
}
