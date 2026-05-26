# Craqle

Craqle is an experimental Rust library for storing, validating, querying, searching, and replicating RO-Crates as RDF named graphs.

The model is simple: one RO-Crate is one named RDF graph. RO-Crate JSON-LD and SPARQL both work against that same graph state. Full-text search is built on Tantivy. Replication uses Irokle graph topics plus OR-Set CRDT semantics over RDF quad changes. Invalid visible RO-Crates are not exported.

This is still early work. Expect breaking changes to the API, storage layout, and replication behavior. Search is intentionally minimal. The workspace currently depends on the `arunaengine/ro-crate-rs` fork on branch `feat/rdfperformance` with the `rdf` feature enabled.

- create and update RO-Crates as named RDF graphs
- import and export RO-Crate JSON-LD
- query and update with SPARQL
- do full-text search with Tantivy
- replicate changes over one Irokle topic per graph
- reject invalid visible crate states on export

## Irokle Sync

Craqle can publish graph events into an external Irokle node that is shared with other applications:

```rust
let irokle = irokle::Irokle::builder()
    .with_fjall_path("./data/irokle")?
    .build()?;
let node = CraqleNode::open_with_options(
    "./data/craqle",
    CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
)?;
```

Each graph gets its own Irokle topic. Local writes are published as durable `CraqleGraphEvent` records first, then reduced into Craqle's RDF projection. After Irokle transport sync receives remote topic data, call `reconcile_irokle()` to apply new graph events locally.

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
- Search is intentionally minimal even though it uses Tantivy; for richer results you still hydrate metadata from RDF.
- Irokle transport integration is library-level; Craqle does not provide a standalone sync server.
- The workspace currently depends on the `arunaengine/ro-crate-rs` fork on branch `feat/rdfperformance` with the `rdf` feature enabled.

There is also a small demo in `examples/demo.rs`:

```bash
cargo run --example demo
```

## License

MIT
