#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use craqle::{
    AppendDataEntitiesReport, CraqleNode, EncodedTerm, GrantAuthorizer, GraphId, GraphPolicy,
    NewDataEntity, PermissionGrant, PermissionLevel,
};
use oxrdf::{NamedNode, Term};

use super::sim::CraqleCluster;

pub fn setup_network(peers: usize) -> (tempfile::TempDir, CraqleCluster) {
    let tmp = tempfile::tempdir().unwrap();
    let net = CraqleCluster::new(peers, tmp.path()).unwrap();
    (tmp, net)
}

pub fn writer_auth() -> GrantAuthorizer {
    writer_auth_for("/tests/**")
}

pub fn bench_auth() -> GrantAuthorizer {
    writer_auth_for("/bench/**")
}

pub fn writer_auth_for(path: &str) -> GrantAuthorizer {
    GrantAuthorizer::new(vec![PermissionGrant::new(path, PermissionLevel::Write)])
}

pub fn public_policy() -> GraphPolicy {
    public_policy_for("/tests/public")
}

pub fn bench_policy() -> GraphPolicy {
    public_policy_for("/bench/public")
}

pub fn public_policy_for(path: &str) -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec![path.to_string()],
    }
}

pub fn literal_term(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
}

pub fn benchmark_media_object_entities(
    start: usize,
    count: usize,
    keyword: &str,
    name_prefix: &str,
    description_label: &str,
    identifier_prefix: &str,
) -> Vec<NewDataEntity> {
    let description = NamedNode::new_unchecked("http://schema.org/description");
    let keywords = NamedNode::new_unchecked("http://schema.org/keywords");
    let identifier = NamedNode::new_unchecked("http://schema.org/identifier");

    let mut entities = Vec::with_capacity(count);
    for idx in start..start + count {
        entities.push(NewDataEntity {
            entity_id: format!("./bulk/entity-{idx:06}.dat"),
            entity_type: "http://schema.org/MediaObject".to_string(),
            name: format!("{name_prefix} {idx}"),
            additional_triples: vec![
                (
                    description.clone(),
                    Term::Literal(oxrdf::Literal::new_simple_literal(format!(
                        "{keyword} {description_label} {idx}"
                    ))),
                ),
                (
                    keywords.clone(),
                    Term::Literal(oxrdf::Literal::new_simple_literal(keyword)),
                ),
                (
                    identifier.clone(),
                    Term::Literal(oxrdf::Literal::new_simple_literal(format!(
                        "{identifier_prefix}-{idx:06}"
                    ))),
                ),
            ],
        });
    }

    entities
}

pub fn benchmark_rocrate_document(
    graph: &GraphId,
    entity_count: usize,
    keyword: &str,
    dataset_name: &str,
) -> String {
    let mut entries = Vec::with_capacity(entity_count + 2);
    entries.push(serde_json::json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
        "about": { "@id": graph.as_str() }
    }));

    let has_part = (0..entity_count)
        .map(|idx| {
            serde_json::json!({
                "@id": format!("./bulk/entity-{idx:06}.dat")
            })
        })
        .collect::<Vec<_>>();

    entries.push(serde_json::json!({
        "@id": graph.as_str(),
        "@type": "Dataset",
        "name": dataset_name,
        "description": format!("{dataset_name} benchmark import"),
        "datePublished": "2025-01-01",
        "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
        "keywords": keyword,
        "hasPart": has_part,
    }));

    for idx in 0..entity_count {
        entries.push(serde_json::json!({
            "@id": format!("./bulk/entity-{idx:06}.dat"),
            "@type": "MediaObject",
            "name": format!("Imported Entity {idx}"),
            "description": format!("{keyword} imported record {idx}"),
            "keywords": keyword,
            "identifier": format!("DOC-{idx:06}"),
        }));
    }

    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": entries,
    })
    .to_string()
}

pub fn append_benchmark_media_objects(
    node: &CraqleNode,
    auth: &GrantAuthorizer,
    graph: &GraphId,
    start: usize,
    count: usize,
    keyword: &str,
) -> AppendDataEntitiesReport {
    node.append_new_root_data_entities(
        auth,
        graph,
        benchmark_media_object_entities(
            start,
            count,
            keyword,
            "Proteomics sample",
            "benchmark record",
            "BENCH",
        ),
    )
    .unwrap()
}

pub fn attach_contextual_entities(
    node: &CraqleNode,
    auth: &GrantAuthorizer,
    graph: &GraphId,
    scope: &str,
    contextual_count: usize,
    label_prefix: &str,
) {
    for ctx_idx in 0..contextual_count {
        let (entity_id, entity_type, name) = match ctx_idx % 3 {
            0 => (
                format!("#person-{scope}-{ctx_idx:02}"),
                "http://schema.org/Person",
                format!("{label_prefix} Person {scope}-{ctx_idx}"),
            ),
            1 => (
                format!("#org-{scope}-{ctx_idx:02}"),
                "http://schema.org/Organization",
                format!("{label_prefix} Org {scope}-{ctx_idx}"),
            ),
            _ => (
                format!("#grant-{scope}-{ctx_idx:02}"),
                "http://schema.org/Grant",
                format!("{label_prefix} Grant {scope}-{ctx_idx}"),
            ),
        };

        node.add_contextual_entity(auth, graph, &entity_id, entity_type, &name)
            .unwrap();

        if ctx_idx < 3 {
            let predicate = match ctx_idx {
                0 => "http://schema.org/creator",
                1 => "http://schema.org/publisher",
                _ => "http://schema.org/funder",
            };
            node.insert_quads(
                graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&NamedNode::new_unchecked(predicate)),
                    EncodedTerm::from_named_node(&NamedNode::new_unchecked(&entity_id)),
                )],
            )
            .unwrap();
        }
    }
}

pub fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

pub fn env_usize_list(key: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|entry| entry.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

pub fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

pub fn format_stats(label: &str, samples: &[Duration]) -> String {
    if samples.is_empty() {
        return format!("{label}: n=0");
    }

    let mut sorted = samples.to_vec();
    sorted.sort();
    format!(
        "{label}: n={}, mean {:?}, p50 {:?}, p95 {:?}, max {:?}",
        sorted.len(),
        mean_duration(&sorted),
        percentile(&sorted, 50),
        percentile(&sorted, 95),
        sorted.last().copied().unwrap_or_default(),
    )
}

pub fn mean_duration(samples: &[Duration]) -> Duration {
    if samples.is_empty() {
        return Duration::default();
    }

    let total_secs: f64 = samples.iter().map(Duration::as_secs_f64).sum();
    Duration::from_secs_f64(total_secs / samples.len() as f64)
}

pub fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    if samples.is_empty() {
        return Duration::default();
    }

    let index = ((samples.len() - 1) * percentile) / 100;
    samples[index]
}

pub fn sum_durations(samples: &[Duration]) -> Duration {
    Duration::from_secs_f64(samples.iter().map(Duration::as_secs_f64).sum())
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

pub fn mean_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.iter().sum::<u64>() / values.len() as u64
}

pub fn dir_size_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }

    let mut total = 0u64;
    let mut stack = vec![PathBuf::from(path)];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                total += entry.metadata().unwrap().len();
            }
        }
    }
    total
}
