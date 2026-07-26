use std::error::Error;
use std::fs;

use craqle::{
    CraqleNode, GrantAuthorizer, GraphId, GraphPolicy, PermissionGrant, PermissionLevel,
    SearchRequest,
};

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("craqle-demo-{}", std::process::id()));
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;

    let node = CraqleNode::open(&root)?;
    let writer = GrantAuthorizer::new(vec![PermissionGrant::new(
        "/demo/**",
        PermissionLevel::Write,
    )]);
    let reader = GrantAuthorizer::default();

    println!("=== Craqle Demo ===\n");

    let graph = GraphId::new("urn:crate:experiment-1");
    node.create_crate(
        &writer,
        craqle::CreateCrateRequest::new(
            graph.clone(),
            "Genomic Analysis of E. coli",
            "Whole-genome sequencing data and analysis pipeline",
            "2025-01-15",
            Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            GraphPolicy {
                public: true,
                permission_paths: vec!["/demo/public/experiment-1".to_string()],
            },
        ),
    )?;
    println!("[Node]  Created crate: Genomic Analysis of E. coli");

    node.add_data_entity(
        &writer,
        &graph,
        "data/sample1.fastq",
        "http://schema.org/MediaObject",
        "Sample 1 FASTQ",
    )?;
    node.add_data_entity(
        &writer,
        &graph,
        "analysis/pipeline.nf",
        "http://schema.org/MediaObject",
        "Nextflow Pipeline",
    )?;
    println!("[Node]  Added two data entities\n");

    let jsonld = node.export_rocrate(&reader, &graph)?;
    println!("[Export] JSON-LD:\n{jsonld}\n");

    node.reindex_search()?;
    let hits = node.search(
        &reader,
        SearchRequest {
            query: "genomic",
            limit: 10,
        },
    )?;
    println!("[Search] Results for 'genomic': {} hits", hits.len());
    for hit in &hits {
        println!("  - {} (score: {:.2})", hit.subject_iri, hit.score);
    }

    println!("\n=== Craqle demo complete ===");
    Ok(())
}
