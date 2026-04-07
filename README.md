# Craqle

Craqle is an experimental Rust library for storing, validating, querying, searching, and replicating RO-Crates as RDF named graphs.

The model is simple: one RO-Crate is one named RDF graph. RO-Crate JSON-LD and SPARQL both work against that same graph state. Full-text search is built on Tantivy. Replication uses OR-Set CRDT semantics over RDF quad changes, with vector clocks for causality. Invalid visible RO-Crates are not exported.

This is still early work. Expect breaking changes to the API, storage layout, and replication behavior. There is no built-in production server or transport yet, search is intentionally minimal, and the replication layer is still low-level. The workspace currently depends on the `arunaengine/ro-crate-rs` fork on branch `feat/rdfperformance` with the `rdf` feature enabled.

- create and update RO-Crates as named RDF graphs
- import and export RO-Crate JSON-LD
- query and update with SPARQL
- do full-text search with Tantivy
- replicate changes between peers with vector clocks and OR-Set CRDT semantics
- reject invalid visible crate states on export

## Examples

Create a crate and export it:

```rust
let node = CraqleNode::open("./data/craqle")?;

let writer = GrantAuthorizer::new(vec![PermissionGrant::new(
    "/datasets/**",
    PermissionLevel::Write,
)]);

let reader = GrantAuthorizer::default();
let graph = GraphId::new("urn:crate:proteomics-study-2025");

node.create_crate(
    &writer,
    CreateCrateRequest::new(
        graph.clone(),
        "Proteomics Study 2025",
        "Mass spectrometry analysis of 200 patient samples",
        "2025-03-26",
        "https://creativecommons.org/licenses/by/4.0/",
        GraphPolicy {
            public: true,
            permission_paths: vec!["/datasets/public/proteomics-study-2025".to_string()],
        },
    ),
)?;

node.append_new_root_data_entities(
    &writer,
    &graph,
    vec![NewDataEntity {
        entity_id: "data/run-01.fastq.gz".into(),
        entity_type: "http://schema.org/MediaObject".into(),
        name: "Run 01 FASTQ".into(),
        additional_triples: Vec::new(),
    }],
)?;

let jsonld = node.export_rocrate(&reader, &graph)?;
```

Query it with SPARQL:

```rust
let rows = node.query(
    &reader,
    "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }",
)?;
```

Search it with Tantivy-backed full-text search:

```rust
let hits = node.search(&reader, "proteomics", 10)?;
let hydrated = node.search_resources(&reader, "proteomics", 10)?;
```

Preview and apply a full RO-Crate JSON-LD update:

```rust
let changes = node.preview_rocrate_update(&writer, &graph, updated_jsonld)?;
let batch = node.apply_rocrate_document(&writer, graph.clone(), updated_jsonld)?;
```

## Limitations

- The API is still moving and there are no stability guarantees yet.
- There is no built-in production server or network transport.
- Search is intentionally minimal even though it uses Tantivy; for richer results you still hydrate metadata from RDF.
- The replication layer is low-level and not a finished sync product.
- The workspace currently depends on the `arunaengine/ro-crate-rs` fork on branch `feat/rdfperformance` with the `rdf` feature enabled.

There is also a small demo in `examples/demo.rs`:

```bash
cargo run --example demo
```

## License

MIT
