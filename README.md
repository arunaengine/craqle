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
