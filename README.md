# Craqle

Craqle is a Rust library for storing and working with RO-Crates as RDF named
graphs.

Use it to create, import, query, update, validate, search, export, and
synchronize RO-Crates. Every read and write accepts an `Authorizer`, so graph
access follows the application's permissions.

## Open a node

The examples below build on this setup:

```rust
use craqle::{AllowAllAuthorizer, CraqleNode};

let node = CraqleNode::open("./data/craqle")?;
let auth = AllowAllAuthorizer;
```

## Create a crate

```rust
use craqle::{CreateCrateRequest, GraphId, GraphPolicy};

let graph = GraphId::new("urn:crate:proteomics");
node.create_crate(
    &auth,
    CreateCrateRequest::new(
        graph.clone(),
        "Proteomics Study",
        "Mass spectrometry data and analysis",
        "2026-08-22",
        None,
        GraphPolicy::default(),
    ),
)?;
```

## Add data

```rust
node.add_data_entity(
    &auth,
    &graph,
    "data/run-01.fastq.gz",
    "http://schema.org/MediaObject",
    "Run 01 FASTQ",
)?;
```

## Import and export JSON-LD

```rust
let document = std::fs::read_to_string("ro-crate-metadata.json")?;
let imported = GraphId::new("urn:crate:imported");

node.apply_rocrate_document_with_policy(
    &auth,
    imported.clone(),
    &document,
    GraphPolicy::default(),
)?;

let jsonld = node.export_rocrate(&auth, &imported)?;
```

## Query with SPARQL

```rust
let results = node.query(
    &auth,
    "SELECT ?entity ?name WHERE {
       ?entity <http://schema.org/name> ?name
     }",
)?;
```

Select the exact graphs used by a query:

```rust
let results = node.query_in_graphs(
    &auth,
    &[graph.clone(), imported.clone()],
    "SELECT ?graph ?name WHERE {
       GRAPH ?graph {
         ?entity <http://schema.org/name> ?name
       }
     }",
)?;
```

Prepare a query for repeated execution:

```rust
use craqle::QueryOptions;

let prepared = node.prepare_query(
    "SELECT ?name WHERE { ?entity <http://schema.org/name> ?name }",
)?;
let execution =
    node.execute_prepared(&auth, &prepared, &QueryOptions::default())?;
```

## Update with SPARQL

```rust
node.apply_sparql_update(
    &auth,
    r#"
      INSERT DATA {
        GRAPH <urn:crate:proteomics> {
          <urn:crate:proteomics>
            <http://schema.org/keywords>
            "proteomics"
        }
      }
    "#,
)?;
```

## Search

```rust
use craqle::SearchRequest;

let hits = node.search(
    &auth,
    SearchRequest {
        query: "proteomics",
        limit: 10,
    },
)?;

let resources = node.search_resources(
    &auth,
    SearchRequest {
        query: "workflow",
        limit: 10,
    },
)?;
```

## Validate with SHACL

```rust
use craqle::{ShaclCompileOptions, ShaclValidationOptions};

let schema = node.compile_shacl(
    &auth,
    &shapes_graph,
    &ShaclCompileOptions::default(),
)?;
let report = node.validate_shacl(
    &auth,
    &data_graph,
    &schema,
    &ShaclValidationOptions::default(),
)?;
```

Bind shapes to a graph and enforce them on writes:

```rust
use craqle::{
    ShaclBinding, ShaclBindingOptions, ShaclWritePolicy,
};

node.bind_shacl(
    &auth,
    &ShaclBinding {
        data_graph: data_graph.clone(),
        shapes_graph: shapes_graph.clone(),
        policy: ShaclWritePolicy::Enforce,
        validation_options: ShaclBindingOptions::default(),
    },
)?;

let statuses = node.shacl_binding_statuses(&auth, &data_graph)?;
```

## Use application permissions

```rust
use craqle::{GrantAuthorizer, PermissionGrant, PermissionLevel};

let writer = GrantAuthorizer::new(vec![PermissionGrant::new(
    "/datasets/project-a/**",
    PermissionLevel::Write,
)]);

node.create_crate(
    &writer,
    CreateCrateRequest::new(
        GraphId::new("urn:crate:project-a"),
        "Project A",
        "Authorized project data",
        "2026-08-22",
        None,
        GraphPolicy {
            public: false,
            permission_paths: vec!["/datasets/project-a/crate".to_string()],
        },
    ),
)?;
```

## Runnable examples

```bash
cargo run --example demo
cargo run --all-features --example api_workflow
```

See `examples/demo.rs` for a small create/export/search workflow and
`examples/api_workflow.rs` for a complete create/import/query/update/SHACL
workflow.
