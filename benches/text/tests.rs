//! Checks persisted text updates, scoring, permissions, and index layouts.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "joins.rs"]
mod joins;
#[path = "mod.rs"]
mod text;

#[cfg(test)]
mod cases {
    use std::cell::Cell;

    use fjall::PersistMode;
    use tantivy::columnar::{BytesColumn, Column};
    use tantivy::fieldnorm::FieldNormReader;
    use tantivy::query::{Bm25StatisticsProvider as Bm25Stats, Bm25Weight, QueryParser};
    use tantivy::schema::{
        FAST, IndexRecordOption, STORED, STRING, Schema, TEXT, TextFieldIndexing, Value,
    };
    use tantivy::tokenizer::{
        AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
        TokenStream,
    };
    use tantivy::{Index, TantivyDocument, Term, doc};

    use super::joins;

    use super::text::{
        ActiveStats, DocumentInput, DocumentKey, EngineError, EngineOptions, FjallBm25,
        HitIdentity, PostingLayout, QueryBounds, ScoreRequest, SearchReport, SearchRequest,
        StatsRequest, TantivyRequest, UpdateWork, WorkCount, collect_full, collect_pruned,
    };

    struct TextFields {
        key: tantivy::schema::Field,
        graph: tantivy::schema::Field,
        subject: tantivy::schema::Field,
        text: tantivy::schema::Field,
        stable: tantivy::schema::Field,
        visible: tantivy::schema::Field,
    }

    struct TestMeta {
        stable: BytesColumn,
        visible: Column<u64>,
    }

    #[test]
    fn updates_reopen() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        let first: UpdateWork = engine
            .upsert(doc_input("g1", "s1", "alpha alpha beta"))
            .unwrap();
        assert_eq!(1, first.generation);
        assert_eq!(first.terms, first.postings);
        assert!(first.postings > 0);
        let second: UpdateWork = engine.upsert(doc_input("g2", "s2", "beta gamma")).unwrap();
        assert_eq!(2, second.generation);
        assert_eq!(second.terms, second.postings);
        assert_eq!(2, engine.generation().unwrap());
        assert_eq!(1, search(&engine, "alpha").len());

        let replaced: UpdateWork = engine.upsert(doc_input("g1", "s1", "gamma")).unwrap();
        assert_eq!(3, replaced.generation);
        assert_eq!(replaced.terms, replaced.postings);
        assert!(search(&engine, "alpha").is_empty());
        assert_eq!(2, search(&engine, "gamma").len());
        engine.persist(PersistMode::SyncAll).unwrap();
        drop(engine);

        let reopened = open_engine(root.path(), PostingLayout::Simple);
        assert_eq!(3, reopened.generation().unwrap());
        assert_eq!(2, search(&reopened, "gamma").len());
        let deleted = reopened.delete(doc_key("g2", "s2")).unwrap();
        assert_eq!(4, deleted.generation);
        assert_eq!(deleted.terms, deleted.postings);
        assert!(deleted.postings > 0);
        assert_eq!(1, search(&reopened, "gamma").len());
        assert_eq!(4, reopened.delete(doc_key("g2", "s2")).unwrap().generation);
    }

    #[test]
    fn layouts_match() {
        let simple_root = tempfile::tempdir().unwrap();
        let block_root = tempfile::tempdir().unwrap();
        let simple = open_engine(simple_root.path(), PostingLayout::Simple);
        let block = open_engine(block_root.path(), PostingLayout::Block { docs: 4 });
        let mut identities = Vec::new();
        for index in 0..19 {
            let graph = format!("graph{}", index % 3);
            let subject = format!("subject{index}");
            let text = if index % 5 == 0 {
                "common rare rare"
            } else {
                "common filler"
            };
            simple.upsert(doc_input(&graph, &subject, text)).unwrap();
            block.upsert(doc_input(&graph, &subject, text)).unwrap();
            identities.push((graph, subject));
        }
        simple
            .upsert(doc_input("graph1", "subject7", "rare replacement"))
            .unwrap();
        block
            .upsert(doc_input("graph1", "subject7", "rare replacement"))
            .unwrap();

        for query in ["common", "rare", "common OR rare"] {
            let left = search(&simple, query);
            let right = search(&block, query);
            assert_hits(&left, &right);
        }
        let keys: Vec<_> = identities
            .iter()
            .map(|(graph, subject)| doc_key(graph, subject))
            .collect();
        let left = simple
            .score_candidates(ScoreRequest {
                query: "common OR rare",
                documents: &keys,
                bounds: QueryBounds::default(),
            })
            .unwrap();
        let exhaustive = simple
            .search(SearchRequest {
                query: "common OR rare",
                limit: keys.len(),
                bounds: QueryBounds::default(),
                allows: None,
            })
            .unwrap();
        let right = block
            .score_candidates(ScoreRequest {
                query: "common OR rare",
                documents: &keys,
                bounds: QueryBounds::default(),
            })
            .unwrap();
        assert_hits(&exhaustive.hits, &left.hits);
        assert_hits(&left.hits, &right.hits);
    }

    #[test]
    fn syntax_is_explicit() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        engine.upsert(doc_input("g", "s", "alpha beta")).unwrap();
        for query in ["alpha beta", "alpha AND beta", "alpha*", "(alpha)"] {
            let error = engine
                .search(SearchRequest {
                    query,
                    limit: 10,
                    bounds: QueryBounds::default(),
                    allows: None,
                })
                .unwrap_err();
            assert!(matches!(error, EngineError::Query(_)));
        }
        let error = engine
            .search(SearchRequest {
                query: "alpha OR beta",
                limit: 10,
                bounds: QueryBounds {
                    terms: 1,
                    ..QueryBounds::default()
                },
                allows: None,
            })
            .unwrap_err();
        assert!(matches!(error, EngineError::Limit { .. }));
        let error = engine
            .search(SearchRequest {
                query: "alpha",
                limit: 10,
                bounds: QueryBounds {
                    postings: 0,
                    ..QueryBounds::default()
                },
                allows: None,
            })
            .unwrap_err();
        assert!(matches!(error, EngineError::Limit { .. }));
    }

    #[test]
    fn eligibility_precedes_topk() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        engine
            .upsert(doc_input("hidden", "s1", "needle needle needle"))
            .unwrap();
        engine.upsert(doc_input("visible", "s2", "needle")).unwrap();
        let allows = |graph: &str| graph == "visible";
        let report: SearchReport = engine
            .search(SearchRequest {
                query: "needle",
                limit: 1,
                bounds: QueryBounds::default(),
                allows: Some(&allows),
            })
            .unwrap();
        assert_eq!(1, report.hits.len());
        assert_eq!("visible", report.hits[0].graph);
        let work: WorkCount = report.work;
        assert_eq!(2, work.posting_rows);
        assert_eq!(1, work.candidate_docs);
        assert_eq!(1, work.rejected_docs);
        assert_eq!(2, work.metadata_reads);
        assert_eq!(1, work.scored_docs);
        assert!(work.bytes <= QueryBounds::default().bytes);
    }

    #[test]
    fn fieldnorm_matches() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        let text = std::iter::repeat_n("needle", 41)
            .collect::<Vec<_>>()
            .join(" ");
        engine.upsert(doc_input("g", "s", &text)).unwrap();
        let hit = search(&engine, "needle").pop().unwrap();
        let length = analyzed_len(&format!("g s {text}"));
        let expected = Bm25Weight::for_one_term_without_explain(1, 1, length as f32)
            .score(FieldNormReader::fieldnorm_to_id(length), 41);
        assert_eq!(expected.to_bits(), hit.score.to_bits());
    }

    #[test]
    fn pruning_matches_full() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        let (index, fields) = text_index();
        let mut writer = index.writer(15_000_000).unwrap();
        for id in 0..24 {
            let graph_value = if id % 4 == 0 { "hidden" } else { "visible" };
            let subject_value = format!("subject{id:02}");
            let body = if id % 3 == 0 {
                "alpha beta beta"
            } else {
                "alpha filler"
            };
            engine
                .upsert(doc_input(graph_value, &subject_value, body))
                .unwrap();
            let all_text = format!("{graph_value} {subject_value} {body}");
            writer
                .add_document(doc!(
                    fields.graph => graph_value,
                    fields.subject => subject_value.clone(),
                    fields.text => all_text,
                    fields.stable => hit_key(graph_value, &subject_value).as_slice(),
                    fields.visible => if graph_value == "visible" { 1u64 } else { 0u64 },
                ))
                .unwrap();
        }
        writer.commit().unwrap();
        let long_body = std::iter::repeat_n("beta", 200)
            .collect::<Vec<_>>()
            .join(" ");
        let long_text = format!("visible subject01 alpha {long_body}");
        writer
            .add_document(doc!(
                fields.graph => "visible",
                fields.subject => "subject01",
                fields.text => long_text,
                fields.stable => hit_key("visible", "subject01").as_slice(),
                fields.visible => 1u64,
            ))
            .unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(&index, vec![fields.text]);
        let query = parser.parse_query("alpha OR beta").unwrap();
        let stats: ActiveStats = engine
            .load_stats(StatsRequest {
                field: fields.text,
                max_terms: 10_000,
                max_bytes: 1 << 20,
            })
            .unwrap();
        assert_eq!(24, stats.docs());
        assert_eq!(24, Bm25Stats::total_num_docs(&stats).unwrap());
        assert_eq!(104, stats.tokens());
        assert_eq!(29, stats.terms());
        assert!(stats.bytes() > 0);
        let metadata: Vec<_> = searcher
            .segment_readers()
            .iter()
            .map(|reader| TestMeta {
                stable: reader.fast_fields().bytes("stable").unwrap().unwrap(),
                visible: reader.fast_fields().u64("visible").unwrap(),
            })
            .collect();
        let candidate = |address: tantivy::DocAddress| {
            let meta = &metadata[address.segment_ord as usize];
            if meta.visible.first(address.doc_id) != Some(1) {
                return Ok(None);
            }
            let stable = first_bytes(&meta.stable, address.doc_id)
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                .ok_or(EngineError::Corrupt("stable key missing"))?;
            Ok(Some(stable))
        };
        let final_reads = Cell::new(0usize);
        let identity = |address| {
            final_reads.set(final_reads.get().saturating_add(1));
            let document: TantivyDocument = searcher.doc(address)?;
            let graph_value = document
                .get_first(fields.graph)
                .and_then(|value| value.as_str())
                .ok_or(EngineError::Corrupt("graph missing"))?;
            assert_ne!("hidden", graph_value);
            let subject_value = document
                .get_first(fields.subject)
                .and_then(|value| value.as_str())
                .ok_or(EngineError::Corrupt("subject missing"))?;
            Ok(HitIdentity {
                graph: graph_value.to_string(),
                subject: subject_value.to_string(),
            })
        };
        let request = || TantivyRequest {
            searcher: &searcher,
            query: query.as_ref(),
            statistics: &stats,
            candidate: &candidate,
            identity: &identity,
            generation: engine.generation().unwrap(),
            limit: 18,
            bounds: QueryBounds::default(),
        };
        let error = collect_full(TantivyRequest {
            searcher: &searcher,
            query: query.as_ref(),
            statistics: &stats,
            candidate: &candidate,
            identity: &identity,
            generation: engine.generation().unwrap(),
            limit: 2,
            bounds: QueryBounds {
                candidates: 1,
                ..QueryBounds::default()
            },
        })
        .unwrap_err();
        assert!(matches!(error, EngineError::Limit { .. }));
        let full: SearchReport = collect_full(request()).unwrap();
        let pruned: SearchReport = collect_pruned(request()).unwrap();
        assert!(full.work.posting_rows <= QueryBounds::default().postings);
        assert!(pruned.work.metadata_reads <= full.work.metadata_reads);
        assert!(!full.work.pruning_fallback);
        assert!(pruned.work.pruning_fallback);
        assert_eq!(full.work.posting_rows, pruned.work.posting_rows);
        assert!(full.work.deduplicated_docs > 0);
        assert_eq!(full.hits.len(), full.work.final_reads);
        assert_eq!(pruned.hits.len(), pruned.work.final_reads);
        assert_eq!(
            full.work.final_reads + pruned.work.final_reads,
            final_reads.get()
        );
        assert_eq!("subject01", full.hits[0].subject);
        assert_ranked(&full.hits);
        assert_hits(&full.hits, &pruned.hits);
    }

    #[test]
    fn join_reinsert_dedupes() {
        let root = tempfile::tempdir().unwrap();
        let engine = open_engine(root.path(), PostingLayout::Simple);
        engine
            .upsert(doc_input("g", "s", "needle replacement"))
            .unwrap();
        let (index, fields) = text_index();
        let mut writer = index.writer(15_000_000).unwrap();
        let key = "g\u{1f}s";
        writer
            .add_document(doc!(
                fields.key => key,
                fields.graph => "g",
                fields.subject => "s",
                fields.text => "g s needle stale",
                fields.stable => hit_key("g", "s").as_slice(),
                fields.visible => 1u64,
            ))
            .unwrap();
        writer.delete_term(Term::from_field_text(fields.key, key));
        writer
            .add_document(doc!(
                fields.key => key,
                fields.graph => "g",
                fields.subject => "s",
                fields.text => "g s needle replacement",
                fields.stable => hit_key("g", "s").as_slice(),
                fields.visible => 1u64,
            ))
            .unwrap();
        writer.commit().unwrap();
        writer
            .add_document(doc!(
                fields.key => key,
                fields.graph => "g",
                fields.subject => "s",
                fields.text => "g s needle replacement",
                fields.stable => hit_key("g", "s").as_slice(),
                fields.visible => 1u64,
            ))
            .unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        let query = QueryParser::for_index(&index, vec![fields.text])
            .parse_query("needle")
            .unwrap();
        let stats = engine
            .load_stats(StatsRequest {
                field: fields.text,
                max_terms: 100,
                max_bytes: 1 << 20,
            })
            .unwrap();
        let candidates = [doc_key("g", "s"), doc_key("g", "s")];
        let report = joins::score_candidates(joins::JoinRequest {
            searcher: &searcher,
            query: query.as_ref(),
            statistics: &stats,
            key_field: fields.key,
            candidates: &candidates,
            generation: engine.generation().unwrap(),
            limit: 10,
            bounds: QueryBounds::default(),
        })
        .unwrap();
        assert_eq!(1, report.hits.len());
        assert_eq!(
            ("g", "s"),
            (
                report.hits[0].graph.as_str(),
                report.hits[0].subject.as_str()
            )
        );
        assert_eq!(2, report.work.candidate_docs);
        assert_eq!(2, report.work.deduplicated_docs);
        assert_eq!(0, report.work.final_reads);
        assert!(report.work.bytes <= QueryBounds::default().bytes);

        let single = joins::score_candidates(joins::JoinRequest {
            searcher: &searcher,
            query: query.as_ref(),
            statistics: &stats,
            key_field: fields.key,
            candidates: &candidates[..1],
            generation: engine.generation().unwrap(),
            limit: 10,
            bounds: QueryBounds::default(),
        })
        .unwrap();
        assert_eq!(2, single.work.candidate_docs);
        let growth = std::mem::size_of::<(tantivy::DocAddress, DocumentKey<'_>)>();
        let payload = "g".len() + "s".len();
        let before_growth = single
            .work
            .bytes
            .saturating_sub(growth)
            .saturating_sub(payload);
        let error = joins::score_candidates(joins::JoinRequest {
            searcher: &searcher,
            query: query.as_ref(),
            statistics: &stats,
            key_field: fields.key,
            candidates: &candidates[..1],
            generation: engine.generation().unwrap(),
            limit: 10,
            bounds: QueryBounds {
                bytes: before_growth + growth / 2,
                ..QueryBounds::default()
            },
        })
        .unwrap_err();
        assert!(matches!(
            error,
            EngineError::Limit {
                resource: "join bytes",
                ..
            }
        ));
    }

    fn open_engine(path: &std::path::Path, layout: PostingLayout) -> FjallBm25 {
        FjallBm25::open(
            path,
            EngineOptions {
                layout,
                ..EngineOptions::default()
            },
        )
        .unwrap()
    }

    fn doc_input<'a>(graph: &'a str, subject: &'a str, text: &'a str) -> DocumentInput<'a> {
        DocumentInput {
            graph,
            subject,
            text,
        }
    }

    fn doc_key<'a>(graph: &'a str, subject: &'a str) -> DocumentKey<'a> {
        DocumentKey { graph, subject }
    }

    fn search(engine: &FjallBm25, query: &str) -> Vec<super::text::SearchHit> {
        engine
            .search(SearchRequest {
                query,
                limit: 100,
                bounds: QueryBounds::default(),
                allows: None,
            })
            .unwrap()
            .hits
    }

    fn assert_hits(left: &[super::text::SearchHit], right: &[super::text::SearchHit]) {
        assert_eq!(left.len(), right.len());
        for (left, right) in left.iter().zip(right) {
            assert_eq!((&left.graph, &left.subject), (&right.graph, &right.subject));
            assert_eq!(left.score.to_bits(), right.score.to_bits());
        }
    }

    fn analyzed_len(text: &str) -> u32 {
        let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build();
        let mut count = 0u32;
        analyzer.token_stream(text).process(&mut |_| count += 1);
        count
    }

    fn text_index() -> (Index, TextFields) {
        let mut schema = Schema::builder();
        let key = schema.add_text_field("key", STRING);
        let graph = schema.add_text_field("graph", STRING | STORED);
        let subject = schema.add_text_field("subject", STRING | STORED);
        let text = schema.add_text_field(
            "text",
            TEXT.set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("craqle_text_v2")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        let stable = schema.add_bytes_field("stable", FAST);
        let visible = schema.add_u64_field("visible", FAST);
        let index = Index::create_in_ram(schema.build());
        index.tokenizers().register(
            "craqle_text_v2",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(AsciiFoldingFilter)
                .build(),
        );
        (
            index,
            TextFields {
                key,
                graph,
                subject,
                text,
                stable,
                visible,
            },
        )
    }

    fn first_bytes(column: &BytesColumn, doc: u32) -> Option<Vec<u8>> {
        let ord = column.term_ords(doc).next()?;
        let mut bytes = Vec::new();
        column
            .ord_to_bytes(ord, &mut bytes)
            .ok()
            .and_then(|found| found.then_some(bytes))
    }

    fn hit_key(graph: &str, subject: &str) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(graph.len() as u64).to_be_bytes());
        hasher.update(graph.as_bytes());
        hasher.update(&(subject.len() as u64).to_be_bytes());
        hasher.update(subject.as_bytes());
        *hasher.finalize().as_bytes()
    }

    fn assert_ranked(hits: &[super::text::SearchHit]) {
        for pair in hits.windows(2) {
            assert!(pair[0].score >= pair[1].score);
            if pair[0].score.to_bits() == pair[1].score.to_bits() {
                assert!(
                    hit_key(&pair[0].graph, &pair[0].subject)
                        <= hit_key(&pair[1].graph, &pair[1].subject)
                );
            }
        }
    }
}
