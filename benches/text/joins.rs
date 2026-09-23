//! Scores an exact RDF candidate set through the original Tantivy query scorer.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};

use tantivy::query::{
    Bm25StatisticsProvider as Bm25Stats, EnableScoring, Query, Scorer, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{DocAddress, DocSet, Searcher, TERMINATED, Term};

use crate::text::{
    DocumentKey, EngineError, QueryBounds, Result, SearchHit, SearchReport, WorkCount, stable_key,
};

pub struct JoinRequest<'a> {
    pub searcher: &'a Searcher,
    pub query: &'a dyn Query,
    pub statistics: &'a dyn Bm25Stats,
    pub key_field: Field,
    pub candidates: &'a [DocumentKey<'a>],
    pub generation: u64,
    pub limit: usize,
    pub bounds: QueryBounds,
}

struct Located<'a> {
    address: DocAddress,
    key: DocumentKey<'a>,
}

#[derive(Clone, Copy)]
struct BestHit<'a> {
    key: DocumentKey<'a>,
    score: f32,
}

pub fn score_candidates(request: JoinRequest<'_>) -> Result<SearchReport> {
    check_limit(
        "join candidates",
        request.bounds.candidates,
        request.candidates.len(),
    )?;
    let mut work = WorkCount::default();
    let capacity = request.candidates.len();
    let retained = capacity_bytes::<Located<'_>>(capacity)
        .saturating_add(hash_bytes::<[u8; 32], ()>(capacity))
        .saturating_add(hash_bytes::<[u8; 32], BestHit<'_>>(capacity))
        .saturating_add(capacity_bytes::<BestHit<'_>>(capacity))
        .saturating_add(capacity_bytes::<SearchHit>(capacity.min(request.limit)));
    charge_bytes(&mut work, request.bounds.bytes, retained)?;
    let mut located = Vec::with_capacity(request.candidates.len());
    let expected = capacity_bytes::<Located<'_>>(capacity);
    let actual = capacity_bytes::<Located<'_>>(located.capacity());
    if actual > expected {
        charge_bytes(&mut work, request.bounds.bytes, actual - expected)?;
    }
    let mut seen = HashSet::with_capacity(capacity);
    for key in request.candidates {
        let stable = stable_key(key.graph, key.subject);
        if !seen.insert(stable) {
            work.deduplicated_docs = work.deduplicated_docs.saturating_add(1);
            continue;
        }
        charge_bytes(
            &mut work,
            request.bounds.bytes,
            key.graph.len().saturating_add(key.subject.len()),
        )?;
        let term = Term::from_field_text(request.key_field, &doc_key(*key));
        let lookup = TermQuery::new(term, IndexRecordOption::Basic);
        let weight = lookup.weight(EnableScoring::disabled_from_searcher(request.searcher))?;
        for (segment, reader) in request.searcher.segment_readers().iter().enumerate() {
            let mut scorer = weight.scorer(reader, 1.0)?;
            while scorer.doc() != TERMINATED {
                let doc = scorer.doc();
                work.posting_rows = work.posting_rows.saturating_add(1);
                work.metadata_reads = work.metadata_reads.saturating_add(1);
                check_limit("join postings", request.bounds.postings, work.posting_rows)?;
                if reader
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                {
                    check_limit(
                        "join located docs",
                        request.bounds.candidates,
                        located.len().saturating_add(1),
                    )?;
                    reserve_located(&mut located, &mut work, request.bounds.bytes)?;
                    located.push(Located {
                        address: DocAddress::new(segment as u32, doc),
                        key: *key,
                    });
                }
                let _ = scorer.advance();
            }
        }
    }
    located.sort_by_key(|item| (item.address.segment_ord, item.address.doc_id));
    let weight = request
        .query
        .weight(EnableScoring::enabled_from_statistics_provider(
            request.statistics,
            request.searcher,
        ))?;
    let mut best: HashMap<[u8; 32], BestHit<'_>> = HashMap::with_capacity(capacity);
    for (segment, reader) in request.searcher.segment_readers().iter().enumerate() {
        let mut scorer = weight.scorer(reader, 1.0)?;
        for item in located
            .iter()
            .filter(|item| item.address.segment_ord == segment as u32)
        {
            work.candidate_docs = work.candidate_docs.saturating_add(1);
            if scorer.seek(item.address.doc_id) != item.address.doc_id {
                continue;
            }
            work.posting_rows = work.posting_rows.saturating_add(1);
            work.scored_docs = work.scored_docs.saturating_add(1);
            check_limit("join scores", request.bounds.postings, work.posting_rows)?;
            let stable = stable_key(item.key.graph, item.key.subject);
            let hit = BestHit {
                key: item.key,
                score: scorer.score(),
            };
            match best.entry(stable) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(hit);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    work.deduplicated_docs = work.deduplicated_docs.saturating_add(1);
                    if hit.score.total_cmp(&entry.get().score).is_gt() {
                        entry.insert(hit);
                    }
                }
            }
        }
    }
    let mut ranked = Vec::with_capacity(best.len());
    ranked.extend(best.into_values());
    sort_best(&mut ranked);
    ranked.truncate(request.limit);
    let mut hits = Vec::with_capacity(ranked.len());
    for hit in ranked {
        charge_bytes(
            &mut work,
            request.bounds.bytes,
            hit.key.graph.len().saturating_add(hit.key.subject.len()),
        )?;
        hits.push(SearchHit {
            graph: hit.key.graph.to_string(),
            subject: hit.key.subject.to_string(),
            score: hit.score,
        });
    }
    work.final_reads = 0;
    Ok(SearchReport {
        generation: request.generation,
        hits,
        work,
    })
}

fn doc_key(key: DocumentKey<'_>) -> String {
    format!("{}\u{1f}{}", key.graph, key.subject)
}

fn sort_best(hits: &mut [BestHit<'_>]) {
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| {
                stable_key(left.key.graph, left.key.subject)
                    .cmp(&stable_key(right.key.graph, right.key.subject))
            })
            .then_with(|| left.key.graph.cmp(right.key.graph))
            .then_with(|| left.key.subject.cmp(right.key.subject))
    });
}

fn capacity_bytes<T>(count: usize) -> usize {
    count.saturating_mul(std::mem::size_of::<T>())
}

fn hash_bytes<K, V>(count: usize) -> usize {
    count
        .saturating_mul(
            std::mem::size_of::<(K, V)>().saturating_add(2 * std::mem::size_of::<usize>()),
        )
        .saturating_mul(2)
}

fn charge_bytes(work: &mut WorkCount, limit: usize, bytes: usize) -> Result<()> {
    work.bytes = work.bytes.saturating_add(bytes);
    check_limit("join bytes", limit, work.bytes)
}

fn reserve_located(
    located: &mut Vec<Located<'_>>,
    work: &mut WorkCount,
    limit: usize,
) -> Result<()> {
    if located.len() < located.capacity() {
        return Ok(());
    }
    let previous = located.capacity();
    let one = std::mem::size_of::<Located<'_>>();
    charge_bytes(work, limit, one)?;
    located
        .try_reserve_exact(1)
        .map_err(|_| EngineError::Limit {
            resource: "join bytes",
            limit,
            actual: usize::MAX,
        })?;
    let added = located
        .capacity()
        .saturating_sub(previous)
        .saturating_mul(one);
    if added > one {
        charge_bytes(work, limit, added - one)?;
    }
    Ok(())
}

fn check_limit(resource: &'static str, limit: usize, actual: usize) -> Result<()> {
    if actual > limit {
        return Err(EngineError::Limit {
            resource,
            limit,
            actual,
        });
    }
    Ok(())
}
