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
