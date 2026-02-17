# External API Examples

This workspace currently exposes its write API through two external-looking input formats:

- SPARQL Update strings via `CraqleNode::apply_sparql_update(...)`
- RO-Crate JSON-LD strings via `CraqleNode::apply_rocrate_document(...)`

The examples below use realistic research-data workflows and keep the payloads external-facing, even though the current integration point is a Rust library call.

Important semantic note:

- JSON-LD imports must use top-level `@graph` entries with `@id` references
- inline nested objects are rejected instead of being silently flattened or skipped
- you can preview the canonical graph diff before applying a full-document update

## Minimal setup

```rust
use craqle::{
    GrantAuthorizer, CraqleNode, GraphId, GraphPolicy, PermissionGrant, PermissionLevel,
};

let tmp = tempfile::tempdir()?;
let node = CraqleNode::open(tmp.path())?;
let graph = GraphId::new("urn:rocrate:proteomics-study-2025");
let auth = GrantAuthorizer::new(vec![PermissionGrant::new(
    "/datasets/**",
    PermissionLevel::Write,
)]);

node.create_crate(
    &auth,
    graph.clone(),
    "Proteomics Study 2025",
    "Mass spectrometry analysis of 200 patient samples.",
    "2025-03-26",
    "https://creativecommons.org/licenses/by/4.0/",
    GraphPolicy {
        public: true,
        permission_paths: vec!["/datasets/public/proteomics-study-2025".to_string()],
    },
)?;
```

## 1. Create a new RO-Crate from JSON-LD

This is the most natural "external system sends a full crate" flow.

```json
{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
      "about": { "@id": "./" }
    },
    {
      "@id": "./",
      "@type": "Dataset",
      "name": "Proteomics Study 2025",
      "description": "Mass spectrometry analysis of 200 patient samples.",
      "datePublished": "2025-03-26",
      "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" }
    }
  ]
}
```

```rust
let jsonld = std::fs::read_to_string("crate.json")?;
let changes = node.preview_rocrate_update(&auth, &graph, &jsonld)?;
println!("document would apply {} graph changes", changes.len());
node.apply_rocrate_document(&auth, graph.clone(), &jsonld)?;
```

## 2. Add a new raw data file with SPARQL

This is a good fit for event-style updates coming from ingest pipelines.

```sparql
PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./> schema:hasPart <./data/raw/run-01.fastq.gz> .
    <./data/raw/run-01.fastq.gz> rdf:type schema:MediaObject .
    <./data/raw/run-01.fastq.gz> schema:name "Run 01 FASTQ" .
    <./data/raw/run-01.fastq.gz> schema:description "Raw reads from sequencing lane 1" .
    <./data/raw/run-01.fastq.gz> schema:encodingFormat "application/gzip" .
    <./data/raw/run-01.fastq.gz> schema:sha256 "6d7d9c8e..." .
  }
}
```

```rust
node.apply_sparql_update(&auth, add_file_sparql)?;
```

## 3. Add a nested analysis folder and result file

This models a common RO-Crate structure where a root dataset contains a derived dataset, and that dataset contains output files.

```sparql
PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./> schema:hasPart <./analysis/> .
    <./analysis/> rdf:type schema:Dataset .
    <./analysis/> schema:name "Differential expression analysis" .
    <./analysis/> schema:description "Normalized and filtered analysis outputs" .

    <./analysis/> schema:hasPart <./analysis/results.tsv> .
    <./analysis/results.tsv> rdf:type schema:MediaObject .
    <./analysis/results.tsv> schema:name "Differential expression table" .
    <./analysis/results.tsv> schema:description "Significant proteins with fold changes" .
    <./analysis/results.tsv> schema:encodingFormat "text/tab-separated-values" .
  }
}
```

```rust
node.apply_sparql_update(&auth, add_analysis_sparql)?;
```

## 4. Add a person and link them as creator

Contextual entities work well via SPARQL too.

```sparql
PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <#alice-smith> rdf:type schema:Person .
    <#alice-smith> schema:name "Dr. Alice Smith" .
    <#alice-smith> schema:affiliation "University Hospital Example" .

    <./> schema:creator <#alice-smith> .
    <./analysis/results.tsv> schema:creator <#alice-smith> .
  }
}
```

```rust
node.apply_sparql_update(&auth, add_creator_sparql)?;
```

## 5. Update a data entity description with SPARQL

This is the typical "fix metadata after QA review" flow.

```sparql
PREFIX schema: <http://schema.org/>

DELETE {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./analysis/results.tsv> schema:description "Significant proteins with fold changes" .
  }
}
INSERT {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./analysis/results.tsv> schema:description "Filtered differential expression results after QC review" .
  }
}
WHERE {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./analysis/results.tsv> schema:description "Significant proteins with fold changes" .
  }
}
```

```rust
node.apply_sparql_update(&auth, update_description_sparql)?;
```

## 6. Add keywords to improve later search

```sparql
PREFIX schema: <http://schema.org/>

INSERT DATA {
  GRAPH <urn:rocrate:proteomics-study-2025> {
    <./> schema:keywords "proteomics" .
    <./> schema:keywords "mass spectrometry" .
    <./analysis/results.tsv> schema:keywords "differential expression" .
  }
}
```

```rust
node.apply_sparql_update(&auth, add_keywords_sparql)?;
```

## 7. Bulk-edit the crate by exporting JSON-LD, modifying it, and re-importing it

This is useful when an external UI or workflow engine edits the full crate document.

```rust
let exported = node.export_rocrate(&GrantAuthorizer::default(), &graph)?;
let mut value: serde_json::Value = serde_json::from_str(&exported)?;

for node in value["@graph"].as_array_mut().unwrap() {
    if node["@id"] == "./analysis/results.tsv" {
        node["name"] = serde_json::Value::String(
            "Differential expression results v2".to_string(),
        );
        node["description"] = serde_json::Value::String(
            "Reprocessed with updated normalization settings".to_string(),
        );
    }
}

let updated_jsonld = serde_json::to_string_pretty(&value)?;
node.apply_rocrate_document(&auth, graph.clone(), &updated_jsonld)?;
```

## 8. Read back the full RO-Crate as JSON-LD

```rust
let jsonld = node.export_rocrate(&GrantAuthorizer::default(), &graph)?;
println!("{jsonld}");
```

## 8b. Search first, then hydrate from RDF

```rust
let hits = node.search(&GrantAuthorizer::default(), "proteomics", 10)?;
let hydrated = node.hydrate_search_hits(&GrantAuthorizer::default(), &hits)?;

for result in hydrated {
    println!("{} in {}", result.hit.subject_iri, result.hit.graph_id);
    for (predicate, object) in result.properties {
        println!("  {} {}", predicate.0, object.0);
    }
}
```

## 9. Read only a lightweight summary for very large crates

This returns the metadata descriptor and root entity, but omits the full list of data entities.

```rust
let summary_jsonld = node.export_rocrate_summary(&GrantAuthorizer::default(), &graph)?;
println!("{summary_jsonld}");
```

## 10. Page through large crates with cursor-based partial loading

This is the preferred large-crate flow because it avoids deep offset paging.

```rust
let first_page = node.export_rocrate_page_after(&GrantAuthorizer::default(), &graph, None, 1000)?;
println!("returned {} of {}", first_page.returned_data_entities, first_page.total_data_entities);

if let Some(cursor) = first_page.next_cursor.as_deref() {
    let second_page = node.export_rocrate_page_after(
        &GrantAuthorizer::default(),
        &graph,
        Some(cursor),
        1000,
    )?;
    println!("next page returned {}", second_page.returned_data_entities);
}
```

The returned page contains:

- `jsonld`: the partial RO-Crate JSON-LD page
- `total_data_entities`: total number of root-linked data entities
- `returned_data_entities`: number of entities in this page
- `next_cursor`: cursor to request the next page

## 11. Offset paging is still available for compatibility

```rust
let page = node.export_rocrate_page(&GrantAuthorizer::default(), &graph, 0, 1000)?;
println!("next offset: {:?}", page.next_offset);
```

For large crates, prefer `export_jsonld_page_after(...)` over `export_jsonld_page(...)`.

## Practical mapping

- Full crate create or replace from an external system: `apply_rocrate_document(...)`
- Preview a full-document change set without committing: `preview_rocrate_update(...)`
- Small targeted metadata updates: `apply_sparql_update(...)`
- Fast read for huge crates: `export_rocrate_summary(...)`
- Fast incremental browsing for huge crates: `export_rocrate_page_after(...)`
- Full document export for downstream tools: `export_rocrate(...)`
- Search then hydrate metadata from RDF: `search(...)` + `hydrate_search_hits(...)`
