#[path = "../benches/support/mod.rs"]
mod corpus;

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use corpus::{
    CorpusConfig, CorpusConfigError, CorpusShape, DEFAULT_SEED, DeterministicCorpus, EntityRole,
    GraphVisibility, ObjectSpec, PredicateKind, REQUIRED_DUPLICATE_PERCENTS, REQUIRED_GRAPH_COUNTS,
    REQUIRED_QUAD_COUNTS,
};

fn config(quads: usize, graphs: usize, duplicate_percent: u8, seed: u64) -> CorpusConfig {
    CorpusConfig::new(quads, graphs, duplicate_percent, seed).unwrap()
}

fn corpus(config: CorpusConfig) -> DeterministicCorpus {
    DeterministicCorpus::new(config).unwrap()
}

#[test]
fn equal_seed_and_config_produce_identical_records() {
    let config = config(10_000, 32, 25, DEFAULT_SEED);
    let left: Vec<_> = corpus(config).into_iter().collect();
    let right: Vec<_> = corpus(config).into_iter().collect();

    assert_eq!(left, right);
}

#[test]
fn changing_seed_changes_the_stream() {
    let left = config(10_000, 32, 0, DEFAULT_SEED);
    let right = config(10_000, 32, 0, DEFAULT_SEED.wrapping_add(1));

    let left: Vec<_> = corpus(left).into_iter().take(128).collect();
    let right: Vec<_> = corpus(right).into_iter().take(128).collect();

    assert_ne!(left, right);
}

#[test]
fn iterator_has_exact_count_and_constant_sized_state() {
    let large = corpus(config(10_000_000, 1_000, 90, DEFAULT_SEED));
    let mut stream = large.into_iter();

    assert!(size_of::<corpus::CorpusIter>() <= 128);
    assert_eq!(stream.len(), 10_000_000);
    assert_eq!(stream.size_hint(), (10_000_000, Some(10_000_000)));
    assert_eq!(stream.next().unwrap().ordinal, 0);
    assert_eq!(stream.len(), 9_999_999);
    assert_eq!(stream.by_ref().take(99).count(), 99);
    assert_eq!(stream.len(), 9_999_900);

    let small = corpus(config(10_000, 1, 0, DEFAULT_SEED));
    assert_eq!(small.into_iter().count(), 10_000);
}

#[test]
fn supported_dimensions_and_impossible_duplicates_are_rejected() {
    assert_eq!(
        CorpusConfig::new(9_999, 32, 0, DEFAULT_SEED),
        Err(CorpusConfigError::UnsupportedQuadCount(9_999))
    );
    assert_eq!(
        CorpusConfig::new(10_000, 8, 0, DEFAULT_SEED),
        Err(CorpusConfigError::UnsupportedGraphCount(8))
    );
    assert_eq!(
        CorpusConfig::new(10_000, 32, 50, DEFAULT_SEED),
        Err(CorpusConfigError::UnsupportedDuplicatePercent(50))
    );
    assert_eq!(
        CorpusConfig::new(10_000, 1, 25, DEFAULT_SEED),
        Err(CorpusConfigError::OneGraphCannotDuplicate {
            graphs: 1,
            duplicate_percent: 25,
        })
    );
}

#[test]
fn required_matrix_contains_only_valid_requested_dimensions() {
    let matrix = CorpusConfig::required_matrix();

    assert_eq!(matrix.len(), 21);
    assert_eq!(
        matrix
            .iter()
            .map(|config| config.quads)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(REQUIRED_QUAD_COUNTS)
    );
    assert_eq!(
        matrix
            .iter()
            .map(|config| config.graphs)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(REQUIRED_GRAPH_COUNTS)
    );
    assert_eq!(
        matrix
            .iter()
            .map(|config| config.duplicate_percent)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(REQUIRED_DUPLICATE_PERCENTS)
    );
    assert!(
        matrix
            .iter()
            .all(|config| config.graphs != 1 || config.duplicate_percent == 0)
    );
}

#[test]
fn graph_coverage_visibility_and_orphan_metadata_are_available() {
    let records: Vec<_> = corpus(config(10_000, 32, 0, DEFAULT_SEED))
        .into_iter()
        .collect();

    let graph_counts = records.iter().fold(BTreeMap::new(), |mut counts, record| {
        *counts.entry(record.graph).or_insert(0usize) += 1;
        counts
    });
    assert_eq!(graph_counts.len(), 32);
    assert!(graph_counts.values().all(|count| *count > 0));
    assert!(
        records
            .iter()
            .any(|record| record.visibility == GraphVisibility::Visible)
    );
    assert!(
        records
            .iter()
            .any(|record| record.visibility == GraphVisibility::Hidden)
    );
    assert!(records.iter().any(|record| record.is_orphan()));
    assert!(
        records
            .iter()
            .filter(|record| record.is_orphan())
            .all(|record| record.role == EntityRole::Orphan)
    );
}

#[test]
fn duplicate_percent_is_exact_and_cross_graph() {
    for graphs in [32, 1_000] {
        for duplicate_percent in [0, 25, 90] {
            let records: Vec<_> = corpus(config(10_000, graphs, duplicate_percent, DEFAULT_SEED))
                .into_iter()
                .collect();
            let expected = 10_000 * duplicate_percent as usize / 100;
            let duplicate_records: Vec<_> =
                records.iter().filter(|record| record.duplicate).collect();

            assert_eq!(duplicate_records.len(), expected);
            for record in duplicate_records {
                let source_ordinal = record
                    .source_ordinal
                    .expect("every duplicate must identify its canonical source");
                let source = &records[source_ordinal];
                assert_ne!(record.graph, source.graph);
                assert_eq!(record.triple_key(), source.triple_key());
            }
        }
    }
}

#[test]
fn canonical_stars_and_chain_segments_stay_local() {
    let records: Vec<_> = corpus(config(10_000, 1_000, 25, DEFAULT_SEED))
        .into_iter()
        .collect();

    let mut star_graphs = BTreeMap::new();
    let mut chain_graphs = BTreeMap::new();
    for record in records.iter().filter(|record| !record.duplicate) {
        match record.shape {
            CorpusShape::SameSubjectStar => {
                if let Some(previous) = star_graphs.insert(record.subject, record.graph) {
                    assert_eq!(previous, record.graph, "star subject escaped its graph");
                }
            }
            CorpusShape::LongChain => {
                let segment = record.ordinal / 128;
                if let Some(previous) = chain_graphs.insert(segment, record.graph) {
                    assert_eq!(previous, record.graph, "chain segment escaped its graph");
                }
            }
            _ => {}
        }
    }
    assert!(!star_graphs.is_empty());
    assert!(!chain_graphs.is_empty());
}

#[test]
fn every_requested_10k_multigraph_configuration_covers_all_graphs() {
    for graphs in [32, 1_000] {
        let expected: BTreeSet<_> = (0..graphs as u32).collect();
        for duplicate_percent in REQUIRED_DUPLICATE_PERCENTS {
            let actual: BTreeSet<_> =
                corpus(config(10_000, graphs, duplicate_percent, DEFAULT_SEED))
                    .into_iter()
                    .map(|record| record.graph)
                    .collect();
            assert_eq!(
                actual, expected,
                "missing graph for {graphs} / {duplicate_percent}%"
            );
        }
    }
}

#[test]
fn all_requested_workload_classes_occur() {
    let records: Vec<_> = corpus(config(10_000, 32, 0, DEFAULT_SEED))
        .into_iter()
        .collect();
    let shapes: BTreeSet<_> = records.iter().map(|record| record.shape).collect();

    assert_eq!(
        shapes,
        BTreeSet::from([
            CorpusShape::SameSubjectStar,
            CorpusShape::LongChain,
            CorpusShape::Orphan,
            CorpusShape::SkewedPredicateObject,
            CorpusShape::RarePredicate,
            CorpusShape::CommonPredicate,
        ])
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.predicate, PredicateKind::Rare(_)))
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.predicate, PredicateKind::Common(_)))
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.object, ObjectSpec::Iri(_)))
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.object, ObjectSpec::Literal(_)))
    );
}

#[test]
fn metadata_is_compact_and_reports_visibility_distribution() {
    let metadata = corpus(config(10_000, 32, 25, DEFAULT_SEED)).metadata();

    assert_eq!(metadata.version, corpus::CORPUS_VERSION);
    assert_eq!(metadata.quads, 10_000);
    assert_eq!(metadata.graphs, 32);
    assert_eq!(metadata.requested_duplicate_quads, 2_500);
    assert_eq!(metadata.hidden_graphs, 8);
    assert_eq!(metadata.visible_graphs, 24);
    assert!(size_of::<corpus::CorpusMetadata>() <= 128);
}

#[test]
fn tiny_golden_prefix_is_stable() {
    let actual = corpus(config(10_000, 32, 25, DEFAULT_SEED))
        .into_iter()
        .take(8)
        .map(|record| {
            format!(
                "{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}\n",
                record.ordinal,
                record.graph,
                record.duplicate,
                record.source_ordinal,
                record.shape,
                record.role,
                record.predicate,
                record.object,
            )
        })
        .collect::<String>();

    assert_eq!(
        actual,
        include_str!("fixtures/performance/corpus-small.golden")
    );
}
