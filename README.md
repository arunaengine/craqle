# Craqle

Craqle is a Rust library for storing, validating, querying, searching, and replicating RO-Crates as RDF named graphs.

The library is built around one core idea:

- one RO-Crate = one named RDF graph
- expanded RDF is the internal source of truth
- RO-Crate JSON-LD and SPARQL are two front doors into the same graph state
- replication operates on CRDT-style quad changes with vector clocks
- invalid visible RO-Crates are never exported

Craqle is now a single crate. The layered internals live under `src/internal/`, but application code should use the root `craqle` API.

## Quick Start

```rust
use craqle::{
    GrantAuthorizer, CraqleNode, CreateCrateRequest, GraphId, GraphPolicy,
    NewDataEntity, PermissionGrant, PermissionLevel,
};

let node = CraqleNode::open("./data/craqle")?;

let writer = GrantAuthorizer::new(vec![PermissionGrant::new(
    "/datasets/**",
    PermissionLevel::Write,
)]);

let reader = GrantAuthorizer::default();
let graph = GraphId::new("urn:crate:proteomics-study-2025");

node.create_crate_with(
    &writer,
    CreateCrateRequest {
        graph: graph.clone(),
        name: "Proteomics Study 2025".into(),
        description: "Mass spectrometry analysis of 200 patient samples".into(),
        date_published: "2025-03-26".into(),
        license: "https://creativecommons.org/licenses/by/4.0/".into(),
        policy: GraphPolicy {
        public: true,
        permission_paths: vec!["/datasets/public/proteomics-study-2025".to_string()],
        },
    },
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

let rows = node.query(
    &reader,
    "SELECT ?name WHERE { ?s schema:name ?name }",
)?;

let jsonld = node.export_rocrate(&reader, &graph)?;
let hits = node.search(&reader, "proteomics", 10)?;
let hydrated = node.hydrate_search_hits(&reader, &hits)?;
let resources = node.search_resources(&reader, "proteomics", 10)?;
```

## Main Types

- `CraqleNode` - main application handle
- `CraqleOptions` - node configuration, including custom rules
- `GraphId` - named graph identifier for one RO-Crate
- `Authorizer` - pluggable authorization hook
- `GrantAuthorizer` - built-in grant/path-based authorizer adapter
- `GraphPolicy` - hidden crate-level policy metadata
- `CreateCrateRequest` - typed input for creating a new RO-Crate
- `CreateEntityRequest` - typed input for creating or replacing an entity
- `NewDataEntity` - typed input for bulk appends
- `RoCratePage` - typed paged export result
- `VectorClock` - causal state per graph for replication and catch-up
- `SyncMessage` - transport-agnostic replication/query message payloads
- `CraqleCluster` - in-process simulation harness for tests and demos

## Stable Integration Surface

For application integration, prefer the root `craqle` API:

- `CraqleNode`
- auth and policy types
- typed create/append/update request structs
- RO-Crate JSON-LD import/export methods
- search/query methods
- `SyncMessage` for replication transport

Lower-level modules such as store, replication, search, and SPARQL internals are available for advanced use cases and tests, but they are not the primary integration surface.

## What To Use When

- use `create_crate_with`, `append_new_root_data_entities`, `export_rocrate`, `preview_rocrate_update`, and `apply_rocrate_document` when your app is RO-Crate centric
- use `apply_sparql_update` and `query` when your app already speaks RDF/SPARQL
- use `search` for full-text discovery, then query the RDF graph for rich metadata
- use `CraqleCluster` only for tests, demos, and simulation; it is not a network transport

## Batch Ingest API

For large append-only ingest, prefer the typed batch API instead of constructing raw change sets:

```rust
use craqle::{NewDataEntity, CraqleNode};

let report = node.append_new_root_data_entities(
    &writer,
    &graph,
    vec![
        NewDataEntity {
            entity_id: "data/run-01.fastq.gz".into(),
            entity_type: "http://schema.org/MediaObject".into(),
            name: "Run 01 FASTQ".into(),
            additional_triples: Vec::new(),
        },
        NewDataEntity {
            entity_id: "data/run-02.fastq.gz".into(),
            entity_type: "http://schema.org/MediaObject".into(),
            name: "Run 02 FASTQ".into(),
            additional_triples: Vec::new(),
        },
    ],
)?;

assert_eq!(report.entity_count, 2);
```

This keeps the public input/output shape much cleaner than passing raw `MaterializedQuadChange` values through application code.

## Authorization Model

Craqle uses hidden crate-level policy metadata.

Each crate has:

- `public: bool`
- `permission_paths: Vec<String>`

Craqle keeps graph policy metadata (`public`, `permission_paths`) but does not require one fixed authorization system.

The root API accepts any `Authorizer` implementation, including closures.

Simple built-in option:

```rust
let reader = GrantAuthorizer::default();
```

Custom service-owned authorization:

```rust
let authorizer = |graph: &craqle::GraphId, policy: &craqle::GraphPolicy, action: craqle::Action| {
    if action == craqle::Action::Read && policy.public {
        return Ok(());
    }

    // Delegate to your service's authz layer here.
    let _ = policy;
    Err(craqle::AuthorizationError::PermissionDenied {
        action,
        graph: graph.as_str().to_string(),
    })
};
```

- `READ` allows reads
- `WRITE` allows writes and also implies `READ`
- the built-in `GrantAuthorizer` uses `globset`

Hidden policy data is excluded from:

- RO-Crate exports
- user-facing SPARQL results
- search indexing
- full-document patch computation

## Rules And Validation

The `rules` module contains the built-in RO-Crate structural rules and also supports future custom rules.

You can attach custom rules through `CraqleOptions`:

```rust
use craqle::{CraqleNode, CraqleOptions, CrateViolation, GraphId, MaterializedQuadChange};
use craqle::rules::{CandidateCheck, GraphSnapshot, Rule};

struct MyRule;

impl Rule for MyRule {
    fn check_candidate(
        &self,
        _store: &craqle::store::GraphStore,
        _graph: &GraphId,
        _delta: &[MaterializedQuadChange],
    ) -> craqle::store::Result<CandidateCheck> {
        Ok(CandidateCheck::Pass)
    }

    fn check_post_state(&self, _post: &GraphSnapshot) -> Result<(), CrateViolation> {
        Ok(())
    }
}

let node = CraqleNode::open_with_options(
    "./data/craqle",
    CraqleOptions::default().with_rule(MyRule),
)?;
```

The rule system is optimized so that rules can evaluate candidate changes directly against the store and delta without always forcing a full graph materialization. The expensive full post-state view is only built when a rule actually needs it, so custom rules can stay cheap on very large graphs.

## RO-Crate Validity And Orphans

Craqle never serves an invalid visible RO-Crate.

When merges disconnect data-entity subtrees:

- the visible active graph stays valid
- disconnected entities are preserved in hidden orphan state
- exports, SPARQL, and search only see the active graph
- hidden diagnostics track orphan warnings and counts

This keeps convergence while preventing invalid user-visible RO-Crate exports.

## Search Model

The search index is intentionally minimal.

Each indexed document stores:

- `doc_key`
- `all_text`

`all_text` includes subject IDs plus text-like literal content. Search returns:

- `graph_id`
- `subject_iri`
- `score`

If you need rich metadata such as names, descriptions, creators, or types, do a follow-up SPARQL query against the RDF graph.

For convenience, the root API also provides `describe_subject(...)`, `hydrate_search_hits(...)`, and `search_resources(...)` so applications can resolve search hits back into RDF properties without bloating the search index.

## RO-Crate Update Planning

Craqle can preview the canonical RDF change set implied by a full RO-Crate document before applying it:

```rust
let changes = node.preview_rocrate_update(&writer, &graph, updated_jsonld)?;
```

This uses the same canonical diff path as `apply_rocrate_document(...)`, so applications can inspect or log the exact graph changes that would be committed.

Unknown compact property names or compact IRIs are rejected instead of being guessed, which keeps the document-update path closer to canonical RDF semantics.

## Working With Existing Fjall State

Craqle can open its own store directory, but it can also be embedded into an already-managed Fjall environment.

```rust
use std::sync::Arc;

use craqle::{CraqleNode, CraqleOptions};
use craqle::search::SearchIndex;
use fjall::Database;

let db = Database::builder("./existing-fjall").open()?;
let search = Arc::new(SearchIndex::open("./existing-search")?);

let node = CraqleNode::from_database_and_search(db, search, CraqleOptions::default())?;
```

This makes it easier to integrate Craqle into an existing application runtime instead of forcing ownership of the whole storage environment.

## Querying And Internal Federation

Node-local queries are always local:

```rust
let rows = node.query(&reader, sparql)?;
```

The simulation layer also supports opt-in internal fanout for multi-node testing:

```rust
let rows = cluster.query_from_peer(0, &reader, sparql, QueryOptions { local_only: false })?;
```

`QueryOptions::local_only` matters only for cluster/simulation fanout helpers.

## Testing

Craqle has two main test layers:

- unit tests inside `src/internal/*.rs` for store, search, SPARQL, and core behavior
- integration suites in `tests/api.rs`, `tests/replication.rs`, `tests/validation.rs`, `tests/search_integration.rs`, `tests/rocrate.rs`, `tests/perf_smoke.rs`, `tests/perf_latency.rs`, and `tests/perf_capacity.rs`

Useful commands:

```bash
cargo test --lib
cargo test --test api
cargo test --test replication
cargo test --workspace
cargo bench --bench partial_loading --no-run
cargo bench --bench large_rocrate_latency --no-run
cargo test --release --test perf_latency large_multi_crate_latency_profile -- --ignored --nocapture
cargo test --release --test perf_capacity single_graph_2_5_million_summary_and_disk_profile -- --ignored --nocapture
cargo test --release --test perf_capacity many_small_graphs_summary_and_disk_profile -- --ignored --nocapture
```

Environment overrides:

- `CRAQLE_BENCH_ENTITY_COUNTS`
- `CRAQLE_HEAVY_ENTITY_COUNT`
- `CRAQLE_HEAVY_BATCH_SIZE`
- `CRAQLE_LARGE_BENCH_CRATE_COUNT`
- `CRAQLE_LARGE_BENCH_ENTITIES_PER_CRATE`
- `CRAQLE_LARGE_BENCH_CONTEXTUALS_PER_CRATE`
- `CRAQLE_LARGE_BENCH_BATCH_SIZE`
- `CRAQLE_PERF_CRATE_COUNT`
- `CRAQLE_PERF_ENTITIES_PER_CRATE`
- `CRAQLE_PERF_CONTEXTUALS_PER_CRATE`
- `CRAQLE_PERF_BATCH_SIZE`
- `CRAQLE_PERF_QUERY_SAMPLES`
- `CRAQLE_CAPACITY_SINGLE_GRAPH_COUNT`
- `CRAQLE_CAPACITY_SINGLE_ENTITIES_PER_GRAPH`
- `CRAQLE_CAPACITY_SINGLE_CONTEXTUALS_PER_GRAPH`
- `CRAQLE_CAPACITY_SINGLE_BATCH_SIZE`
- `CRAQLE_CAPACITY_SINGLE_SUMMARY_SAMPLES`
- `CRAQLE_CAPACITY_MANY_GRAPH_COUNT`
- `CRAQLE_CAPACITY_MANY_ENTITIES_PER_GRAPH`
- `CRAQLE_CAPACITY_MANY_CONTEXTUALS_PER_GRAPH`
- `CRAQLE_CAPACITY_MANY_BATCH_SIZE`
- `CRAQLE_CAPACITY_MANY_SUMMARY_SAMPLES`

## Current Limitations

- transport is abstracted as `SyncMessage`; no production network stack is built in
- search is deliberately minimal and expects metadata hydration from RDF queries
- internal federation is available through simulation helpers, not a production cluster runtime
- the local `ro-crate-rs` dependency must expose the `rdf` feature for Cargo resolution to succeed

## Code Layout

- `src/lib.rs` - public library surface
- `src/internal/core.rs` - types, vector clocks, CRDT ops, vocab
- `src/internal/store.rs` - Fjall-backed RDF store
- `src/internal/sparql.rs` - SPARQL execution and FTS query rewrite
- `src/internal/rules.rs` - built-in and custom validation rules
- `src/internal/replication.rs` - replication and catch-up logic
- `src/internal/rocrate.rs` - RO-Crate import/export layer
- `src/internal/search.rs` - Tantivy-backed search index
- `src/sim.rs` - in-process simulation and internal federation hooks
- `tests/api.rs` - public API integration coverage
- `tests/replication.rs` - replication and convergence scenarios
- `tests/validation.rs` - rule and orphan handling scenarios
- `tests/search_integration.rs` - search and FTS integration scenarios
- `tests/rocrate.rs` - RO-Crate import/export lifecycle scenarios
- `tests/perf_smoke.rs` - ignored performance smoke tests
- `tests/perf_latency.rs` - ignored release-mode end-to-end latency profiles
- `tests/perf_capacity.rs` - ignored release-mode summary and disk-footprint capacity profiles
- `benches/large_rocrate_latency.rs` - Criterion benchmark for multi-crate large-scale reads
- `API_EXAMPLES.md` - longer workflow examples
