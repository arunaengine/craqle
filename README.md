# Craqle

Craqle is an experimental Rust library for storing, validating, querying, searching, and replicating RO-Crates as RDF named graphs.

The model is simple: one RO-Crate is one named RDF graph. RO-Crate JSON-LD and SPARQL both work against that same graph state. Full-text search is built on Tantivy. Irokle graph topics provide the durable operation log and sync obligations, while Craqle reduces those events into an OR-Set RDF projection. Invalid visible RO-Crates are not exported.

This is still early work. Expect breaking changes to the API, storage layout, and replication behavior. Search is intentionally minimal. The workspace depends on `intbio-ncl/ro-crate-rs` tag `v0.6.0` with the `rdf` feature and a local compatibility patch in `vendor/ro-crate-rs-0.6.0`.

- create and update RO-Crates as named RDF graphs
- import and export RO-Crate JSON-LD
- query and update with SPARQL
- compile, bind, and evaluate the optional native `CraqleFastV1` SHACL profile
- do full-text search with Tantivy
- replicate changes over one Irokle topic per graph
- reject invalid visible crate states on export

## Irokle Sync

Craqle publishes graph events into an Irokle node for durable operation history and sync obligations. The Irokle node can be shared with other applications:

```rust
let irokle = irokle::Irokle::builder()
    .with_fjall_path("./data/irokle")?
    .build()?;
let node = CraqleNode::open_with_options(
    "./data/craqle",
    CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
)?;
```

Each graph gets its own Irokle topic. Local writes are published as durable `CraqleGraphEvent` records first, then reduced into Craqle's RDF projection. Opening a node with Irokle configured replays durable graph events before returning, so the Irokle log is authoritative if a process stops after publishing but before projection catches up. After Irokle transport sync receives new remote topic data, call `reconcile_irokle()` to apply those graph events locally.

Enable Craqle's `iroh` feature when the embedded Irokle dependency should include Irokle's Iroh transport and async write-concern obligation scheduling.

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

Enable native SHACL with `--features shacl-core`. Given existing data and
shapes graphs, compile once, validate directly, bind a policy, and read the
persisted status:

```rust
use craqle::{
    AllowAllAuthorizer, ShaclBinding, ShaclBindingOptions,
    ShaclCompileOptions, ShaclValidationOptions, ValidationPolicy,
};

let auth = AllowAllAuthorizer;
let schema = node.compile_shacl(&auth, &shapes_graph, &ShaclCompileOptions::default())?;
let report = node.validate_shacl(
    &auth,
    &data_graph,
    &schema,
    &ShaclValidationOptions::default(),
)?;

let binding = ShaclBinding {
    data_graph: data_graph.clone(),
    shapes_graph: shapes_graph.clone(),
    policy: ValidationPolicy::Enforce,
    validation_options: ShaclBindingOptions::default(),
};
let initial = node.bind_shacl(&auth, &binding)?;
let current = node.shacl_binding_statuses(&auth, &data_graph)?;
```

`Enforce` rejects an invalid local checked write before commit. `Advisory`
commits the write and persists its complete report asynchronously with respect
to the source transition. `Disabled` skips SHACL for that binding; the existing
RO-Crate checks still apply. Replicated CRDT records always apply before local
SHACL settlement.

`CraqleFastV1` supports node, class, subjects-of, objects-of, and implicit-class
targets; direct, inverse, sequence, alternative, zero-or-one, zero-or-more,
and one-or-more paths; and the bounded native constraint set documented in
[`docs/performance-v1/SHACL_SUPPORT.md`](docs/performance-v1/SHACL_SUPPORT.md).
It returns one complete report or one error. SHACL-SPARQL, SHACL-JS, SHACL-AF,
custom components and targets, reifier shapes, RDF-star shapes, remote imports,
and the other components listed in that support document are not supported.

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
- The workspace uses `intbio-ncl/ro-crate-rs` tag `v0.6.0` plus the tracked local compatibility patch.
- `CraqleFastV1` is a deliberately bounded profile, not unrestricted SHACL Core conformance.

There is also a small demo in `examples/demo.rs`:

```bash
cargo run --example demo
```

## License

MIT
