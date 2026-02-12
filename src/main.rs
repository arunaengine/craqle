use anyhow::Result;
use aruna_core::*;
use aruna_sync::SyncNetwork;

fn main() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut net = SyncNetwork::new(3, tmp.path())?;

    println!("=== RO-Crate CRDT Sync Demo ===\n");

    // Peer 0 creates a crate
    let graph = GraphId::new("urn:crate:experiment-1");
    let manager0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
    manager0.create_crate(
        graph.clone(),
        "Genomic Analysis of E. coli",
        "Whole-genome sequencing data and analysis pipeline",
        "2025-01-15",
        "https://creativecommons.org/licenses/by/4.0/",
    )?;
    println!("[Peer 0] Created crate: Genomic Analysis of E. coli");

    // Sync to all peers
    net.sync_until_converged(10)?;
    println!("[Sync]   All peers synchronized\n");

    // Peer 0 adds a data entity
    manager0.add_data_entity(
        &graph,
        "data/sample1.fastq",
        "http://schema.org/MediaObject",
        "Sample 1 FASTQ",
        vec![],
    )?;
    println!("[Peer 0] Added data/sample1.fastq");

    // Peer 1 (offline) adds a different entity
    let manager1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
    manager1.add_data_entity(
        &graph,
        "analysis/pipeline.nf",
        "http://schema.org/MediaObject",
        "Nextflow Pipeline",
        vec![],
    )?;
    println!("[Peer 1] Added analysis/pipeline.nf");

    // Sync and verify convergence
    net.sync_until_converged(10)?;
    println!("[Sync]   All peers converged after concurrent edits\n");

    // Export from peer 2
    let manager2 = aruna_rocrate::RoCrateManager::new(net.peer(2).engine.clone());
    let jsonld = manager2.export_jsonld(&graph)?;
    println!("[Peer 2] Exported JSON-LD:\n{jsonld}\n");

    // Search
    net.reindex_search()?;
    let hits = net.peer(0).search.search("genomic", 10)?;
    println!("[Search] Results for 'genomic': {} hits", hits.len());
    for hit in &hits {
        println!("  - {} (score: {:.2})", hit.subject_iri, hit.score);
    }

    println!("\n=== Demo complete ===");
    Ok(())
}
