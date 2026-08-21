# Craqle

Craqle 0.2 is a versioned Rust library for storing, querying, validating,
searching, and replicating RO-Crates as RDF named graphs.

The model is simple: one RO-Crate is one named RDF graph. RO-Crate JSON-LD and SPARQL both work against that same graph state. Full-text search is built on Tantivy. Irokle graph topics provide the durable operation log and sync obligations, while Craqle reduces those events into an OR-Set RDF projection. Invalid visible RO-Crates are not exported.

`0.2.0-rc.1` is a release candidate for the supported 0.2 API, disk-data,
SPARQL, `CraqleFastV1` SHACL, and RO-Crate contracts. It is not the final 0.2
release.

Within the 0.2.x series:

- documented public APIs remain source compatible unless a correctness or
  security defect makes this impossible;
- authoritative CRDT data written by 0.2 remains readable by later 0.2
  releases;
- derived qv indexes, search indexes, and compiled-schema caches may be
  rebuilt;
- unsupported forms fail explicitly;
- deprecated 0.2 APIs remain until 0.3 unless they create a correctness or
  security issue, and their Rust documentation names the replacement;
- a future 0.3 release may contain breaking changes with a migration note.

| Area | 0.2 status |
| --- | --- |
| RDF named-graph storage | Supported |
| CRDT source data | Supported |
| RO-Crate 1.1, 1.2, and 1.3 import/export | Supported within documented behavior |
| Local SPARQL | Supported |
| qv query indexes | Supported derived data |
| `CraqleFastV1` SHACL | Supported bounded profile |
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
| Deprecated | APIs explicitly carrying a Rust `deprecated` attribute and a documented replacement; retained until 0.3 unless correctness or security requires removal |

Public enums intended to grow are non-exhaustive. Existing names do not change
semantics silently. The committed API snapshot is checked alongside a Rust
semver analysis in CI.

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
and one-or-more paths. Its constraints are min/max count; datatype, node kind,
class, value and enumeration checks; numeric, string, pattern, and language
checks; closed shapes; pairwise property checks; nested logical constraints;
and qualified value constraints. It returns one complete report or one error.
Local imports are opt-in and version-fenced. SHACL-SPARQL, SHACL-JS, SHACL-AF,
custom components and targets, reifier shapes, RDF-star shapes, and remote
imports are unsupported and fail explicitly. Defaults bound reports to 10,000
results, paths to 1,000,000 edges, and path depth to 128.

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
The Criterion p95/p99 values used ten samples and are nearest-rank sample
values, not production tail estimates.

| 1M case | Selected path | Comparison path |
| --- | ---: | ---: |
| Predicate-object ASK | Auto 0.263 ms | Forced source 17.612 ms |
| Predicate-object LIMIT | Auto 0.165 ms | Forced source 16.840 ms |
| Join | Auto/hash about 2.1 s | Forced lateral about 36.5 s |
| Native cached SHACL | 9.9x faster than external copy | Zero data-graph copy bytes |

Historical 10M SPARQL rows remain access-path evidence only. No
current-final-binary 10M SHACL, incremental-validation, or checked-write result
is claimed; that long run remains deferred until explicitly authorized.

## Limitations

- Search is intentionally minimal even though it uses Tantivy; for richer results you still hydrate metadata from RDF.
- Irokle transport integration is library-level; Craqle does not provide a standalone sync server.
- The Git release candidate uses Rudof `0.3.10` from crates.io and pins exact
  revisions of the maintained RO-Crate fork and Irokle. It is not
  crates.io-ready until equivalent registry releases exist.
- `CraqleFastV1` is a deliberately bounded profile, not unrestricted SHACL Core conformance.

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
| `shacl-core` | No | Native bounded `CraqleFastV1` compilation, validation, and policy bindings |
| `iroh` | No | Irokle Iroh transport and asynchronous write-concern scheduling |

There is also a small demo in `examples/demo.rs`:

```bash
cargo run --example demo
```

## License

MIT
