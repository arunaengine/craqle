# Craqle

Craqle is a Linux-first Rust library for storing, querying, validating,
searching, and replicating RO-Crates as RDF named graphs.

Craqle 0.2 defines its public storage, RO-Crate, SPARQL, and Craqle SHACL
Core Subset v1 interfaces. A future 0.3 release may contain breaking changes.

The model is simple: one RO-Crate is one named RDF graph. RO-Crate JSON-LD and SPARQL both work against that same graph state. Full-text search is built on Tantivy. Irokle graph topics provide the durable operation log and sync obligations, while Craqle reduces those events into an OR-Set RDF projection. Invalid visible RO-Crates are not exported.

Linux is the only supported platform for 0.2. Distribution is the `v0.2.0` Git
tag with the exact Git dependency revisions recorded in `Cargo.toml` and
`Cargo.lock`; Craqle 0.2 is not distributed through crates.io.

Within the 0.2.x series:

- documented public APIs remain source compatible unless a correctness or
  security defect makes this impossible;
- authoritative CRDT data written by 0.2 remains readable by later 0.2
  releases;
- derived qv indexes, search indexes, and compiled-schema caches may be
  rebuilt;
- unsupported forms fail explicitly;
- a future 0.3 release may contain breaking changes with a migration note.

| Area | 0.2 status |
| --- | --- |
| RDF named-graph storage | Supported |
| CRDT source data | Supported |
| RO-Crate 1.1, 1.2, and 1.3 import/export | Supported within documented behavior |
| Local SPARQL | Supported |
| qv query indexes | Supported derived data |
| Craqle SHACL Core Subset v1 (`ShaclProfile::CoreSubsetV1`) | Supported bounded profile |
| Enforce and Advisory policies | Supported |
| Full SHACL Core | Not claimed |
| SHACL-SPARQL, JS, AF, remote imports | Unsupported |
| RDF-star | Unsupported |

## Public API policy

| Classification | Surface |
| --- | --- |
| Stable in 0.2.x | Root storage, RO-Crate, authorization, local SPARQL, search, replication, error, and disk-format APIs documented in Rustdoc |
| Feature-gated in 0.2.x | `shacl-core`, `search`, and `iroh` surfaces |
| Internal | Everything under `src/internal/`, encoded storage IDs, physical forcing controls used only by tests and benchmarks |

Public enums intended to grow are non-exhaustive. Existing names do not change
semantics silently.

- create and update RO-Crates as named RDF graphs
- import and export RO-Crate JSON-LD
- query and update with SPARQL
- compile, bind, and evaluate the optional native Craqle SHACL Core Subset v1
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
        Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
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
    ShaclCompileOptions, ShaclValidationOptions, ShaclWritePolicy,
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
    policy: ShaclWritePolicy::Enforce,
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

Craqle SHACL Core Subset v1 (`ShaclProfile::CoreSubsetV1`) supports node,
class, subjects-of, objects-of, and implicit-class targets; direct, inverse,
sequence, alternative, zero-or-one, zero-or-more, and one-or-more paths. Its
constraints are min/max count; datatype, node kind, class, value and
enumeration checks; numeric, string, pattern, and language checks; closed
shapes; pairwise property checks; nested logical constraints; and qualified
value constraints. It returns one complete report or one error.

Local shape imports are opt-in and version-fenced. Recursive shapes,
SHACL-SPARQL, SHACL-JS, SHACL-AF, custom components and targets, reifier
shapes, RDF-star shapes, and remote imports are unsupported and fail
explicitly. Defaults bound reports to 10,000 results, paths to 1,000,000 edges,
and path depth to 128.

`report.conforms` is true exactly when the report has no results. Write
acceptance is separate: `Advisory` never rejects, while `Enforce` rejects at or
above its `ShaclBlockingSeverity` threshold. The default threshold is
`ViolationOnly`; unknown custom severities fail closed under `Enforce` unless
explicitly mapped.

Search it with Tantivy-backed full-text search:

```rust
let hits = node.search(
    &reader,
    SearchRequest { query: "proteomics", limit: 10 },
)?;
let hydrated = node.search_resources(
    &reader,
    SearchRequest { query: "proteomics", limit: 10 },
)?;
```

Preview and apply a full RO-Crate JSON-LD update:

```rust
let changes = node.preview_rocrate_update(&writer, &graph, &updated_jsonld)?;
let batch = node.apply_rocrate_document(&writer, graph.clone(), &updated_jsonld)?;
```

With `shacl-core`, a raw document can be parsed once, checked without changing
the graph, and committed against the same data and shapes version fences:

```rust
use craqle::{
    PrepareRoCrateOptions, PreparedCommitMode, RoCratePolicyOptions,
    ShaclCompileOptions,
};

let policy = node.compile_rocrate_policy(
    &writer,
    &shapes_graph,
    &ShaclCompileOptions::default(),
)?;
let prepared = node.prepare_rocrate_document(
    &writer,
    &graph,
    raw_jsonld,
    &PrepareRoCrateOptions::default(),
)?;
let report = node.evaluate_rocrate_policy(
    &writer,
    &prepared,
    &policy,
    &RoCratePolicyOptions::default(),
)?;
if report.conforms {
    node.commit_prepared_rocrate_document(
        &writer,
        prepared,
        Some(&policy),
        PreparedCommitMode::Enforce,
    )?;
}
```

## Accepted performance snapshot

These are retained same-fixture measurements, not promises for every workload.
The short 10K Craqle/Oxigraph diagnostic reported Craqle faster in every listed
case, with typical gains of roughly 2.5x to 4.9x. One-or-more path was roughly
1.7x faster and exact count was roughly 3.2x faster. The retained triangle
times were 2.54 ms for Craqle and 12.28 ms for Oxigraph: the ratio is about
0.207, so Craqle was about 4.8x faster. The incompatible earlier ratio was an
arithmetic error and is not retained.

Craqle's internal comparison measured the EXISTS count fast path at roughly
25x faster than generic evaluation and NOT EXISTS at roughly 28x faster.
Existing accepted SHACL evidence measured native cached validation at roughly
40x faster at 10K and roughly 9.9x faster at 1M than the external-copy path.

The post-count-change 1M Craqle/Oxigraph comparison was not rerun. The short
10K measurements are diagnostic rather than production-tail results. No new
benchmark was required for the release, and no new benchmark was run.

Tested release scale is therefore the ordinary regression suite plus retained
10K query diagnostics and retained 10K/1M SHACL evidence. Historical 10M
access-path runs are not a 0.2 release claim or gate.

## Limitations

- Search is intentionally minimal even though it uses Tantivy; for richer results you still hydrate metadata from RDF.
- Irokle transport integration is library-level; Craqle does not provide a standalone sync server.
- Linux is the only supported 0.2 platform.
- Distribution uses Git tag `v0.2.0` and the exact Git revisions of the
  maintained RO-Crate fork and Irokle; this release is not a crates.io package.
- Craqle SHACL Core Subset v1 is deliberately bounded and does not claim full
  SHACL Core conformance.

## Disk data and recovery

The authoritative format marker is `1.0`. CRDT source records, the term data
needed to decode them, graph recovery metadata, and committed policy bindings
are authoritative and must be included in a consistent backup. An unknown
future authoritative format, a malformed marker, or an unmarked non-empty
store fails closed at open.

qv query indexes, Tantivy search indexes, and compiled SHACL caches are derived
and disposable. They may be omitted from a backup and rebuilt from
authoritative state. Restoring a database requires an empty destination and
must preserve one consistent view of all authoritative keyspaces.

## Feature flags

| Feature | Default | Behavior |
| --- | --- | --- |
| `search` | Yes | Tantivy full-text derived index |
| `shacl-core` | No | Native Craqle SHACL Core Subset v1 compilation, validation, and policy bindings |
| `iroh` | No | Irokle Iroh transport and asynchronous write-concern scheduling |

`examples/api_workflow.rs` is the compiled end-to-end API example. There is
also a smaller demo in `examples/demo.rs`:

```bash
cargo run --example demo
cargo run --all-features --example api_workflow
```

## License

MIT
