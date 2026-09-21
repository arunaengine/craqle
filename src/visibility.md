<!-- Describes Craqle read boundaries with a runnable crate example. -->
<!-- Copyright (c) 2026 ArunaStorage Team @ JLU Giessen -->
<!-- SPDX-License-Identifier: MIT -->

# Visibility boundaries

Craqle has several read boundaries. They are not one transaction across indexes.

- A SPARQL query reads one store snapshot. Graph policy for default-union
  visibility comes from the same snapshot. The query returns complete rows or an
  error, including when required policy cannot be read.
- Text search reads the last published search view, which can lag behind
  durable writes. Hits are rechecked against a store snapshot before they return.
- [`CraqleNode::flush_search`] waits until search covers every write accepted
  before the call, and its [`SearchReceipt`] reports that target. Writes or
  index repairs scheduled after the call need another flush.
- [`CraqleNode::search_resources`] hydrates hits from current store state and
  rechecks current policy, so a hit can disappear between search and hydration.

Cancellation and timeouts are cooperative: they are checked between units of
work, and an ended budget returns an error instead of a shortened result.

```
# fn main() -> craqle::Result<()> {
use craqle::{
    AllowAllAuthorizer, CraqleErrorKind, CraqleNode, CreateCrateRequest, DenyAllAuthorizer,
    GraphId, GraphPolicy, QueryOptions, QueryResults,
};

let directory = tempfile::tempdir()?;
let node = CraqleNode::open(directory.path())?;
let graph = GraphId::new("urn:example:soil-study");
let request = CreateCrateRequest::new(
    graph.clone(),
    "Soil study",
    "drought samples",
    "2026-09-21",
    None,
    GraphPolicy::default(),
);
node.create_crate(&AllowAllAuthorizer, request)?;

// Prepared once, executed results-only: no per-operator statistics are collected.
let prepared = node.prepare_query("ASK { ?s <http://schema.org/name> \"Soil study\" }")?;
let scope = std::slice::from_ref(&graph);
let options = QueryOptions::results_only();
let run = node.execute_prepared_in_graphs(&AllowAllAuthorizer, scope, &prepared, &options)?;
assert_eq!(run.results, QueryResults::Boolean(true));

// An unreadable explicit graph fails the whole request with a stable error kind.
let denied = node.execute_prepared_in_graphs(&DenyAllAuthorizer, scope, &prepared, &options);
assert_eq!(denied.unwrap_err().kind(), CraqleErrorKind::Unauthorized);

#[cfg(feature = "search")]
{
    use craqle::{GraphSearchRequest, GraphSearchRun, SearchFlushOptions, SearchOptions};

    // Read your writes in search: flush after the write, then search a known graph.
    let receipt = node.flush_search(&SearchFlushOptions::default())?;
    assert!(receipt.covered >= receipt.target);
    let options = SearchOptions {
        timeout: Some(std::time::Duration::from_secs(30)),
        ..SearchOptions::default()
    };
    let request = GraphSearchRequest {
        graphs: scope,
        query: "drought",
        limit: 10,
    };
    let run = GraphSearchRun {
        request,
        options: &options,
    };
    let hits = node.search_graphs_with(&AllowAllAuthorizer, run)?;
    assert_eq!(hits[0].graph_id, graph.as_str());

    // A cancelled request fails even when nothing would match.
    options.cancellation.cancel();
    let request = GraphSearchRequest {
        graphs: scope,
        query: "absent",
        limit: 10,
    };
    let run = GraphSearchRun {
        request,
        options: &options,
    };
    let error = node.search_graphs_with(&AllowAllAuthorizer, run).unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Cancelled);
}
# Ok(())
# }
```

# Diagnostics and errors

Preparing a query saves parsing on each run, but it does not choose how much is measured.
`QueryOptions::default()` collects per-operator statistics; `QueryOptions::results_only()`
does not. A caller that cannot read every graph receives a reduced diagnostic view, marked by
`details_withheld`, instead of zeros that could be mistaken for measurements.

Handle a denial differently from a failure. `Unauthorized` means the caller may not read a
graph. `Storage` and `CorruptDerivedData` mean Craqle could not produce a complete answer;
derived-index damage is repaired automatically, so a later retry can succeed.

```
# fn main() -> craqle::Result<()> {
use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CraqleErrorKind, CraqleNode,
    CreateCrateRequest, DenyAllAuthorizer, GraphId, GraphPolicy, QueryOptions,
};

let directory = tempfile::tempdir()?;
let node = CraqleNode::open(directory.path())?;
let study = GraphId::new("urn:example:diagnostics");
let request = CreateCrateRequest::new(
    study.clone(),
    "Diagnostics study",
    "diagnostic example",
    "2026-09-21",
    None,
    GraphPolicy::default(),
);
node.create_crate(&AllowAllAuthorizer, request)?;
let scope = std::slice::from_ref(&study);
let prepared = node.prepare_query("SELECT ?s WHERE { ?s ?p ?o }")?;

// Detailed statistics are an explicit choice, even for a prepared query.
let detailed = QueryOptions::default();
let full = node.execute_prepared_in_graphs(&AllowAllAuthorizer, scope, &prepared, &detailed)?;
assert!(!full.statistics.details_withheld());

// This caller reads only one graph, so plan details that could reflect others are withheld.
let one_graph = |graph: &GraphId, _: &GraphPolicy, action: Action| {
    if graph.as_str() == "urn:example:diagnostics" {
        Ok(())
    } else {
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_owned(),
        })
    }
};
let reduced = node.execute_prepared_in_graphs(&one_graph, scope, &prepared, &detailed)?;
assert!(reduced.statistics.details_withheld());
assert_eq!(full.statistics.result_rows, reduced.statistics.result_rows);

match node.query_in_graphs(&DenyAllAuthorizer, scope, "ASK { ?s ?p ?o }") {
    Err(error) if error.kind() == CraqleErrorKind::Unauthorized => {}
    Err(error) if error.kind() == CraqleErrorKind::Storage => panic!("retry later: {error}"),
    Err(error) if error.kind() == CraqleErrorKind::CorruptDerivedData => {
        panic!("repair is scheduled; retry later: {error}")
    }
    other => panic!("unexpected outcome: {other:?}"),
}
# Ok(())
# }
```

# RO-Crate graphs and scoped search

A convenient convention stores each crate in its own named graph whose IRI is the crate
root. A query for one crate then names that graph, and a query over a chosen set of crates
passes the set explicitly, so Craqle reads only those graphs' index ranges where it can.

Relative entity ids other than the root are stored as written, for example `./data/a.csv`,
and blank node labels are not renamed per document. The same relative path or label in two
crates therefore names one RDF node. Use absolute IRIs for entities that must stay distinct.

```
# fn main() -> craqle::Result<()> {
use craqle::{
    AllowAllAuthorizer, CraqleNode, CreateCrateRequest, GraphId, GraphPolicy, QueryOptions,
    QueryResults,
};

let directory = tempfile::tempdir()?;
let node = CraqleNode::open(directory.path())?;
let crates: Vec<GraphId> = ["urn:example:crate:1", "urn:example:crate:2"]
    .into_iter()
    .map(GraphId::new)
    .collect();
for (index, graph) in crates.iter().enumerate() {
    let request = CreateCrateRequest::new(
        graph.clone(),
        &format!("Survey {index}"),
        "coastal sediment",
        "2026-09-21",
        None,
        GraphPolicy::default(),
    );
    node.create_crate(&AllowAllAuthorizer, request)?;
}

// One crate: name its graph in the query.
let open = node.prepare_query(
    "SELECT ?name WHERE { GRAPH <urn:example:crate:1> { \
     <urn:example:crate:1> <http://schema.org/name> ?name } }",
)?;
let options = QueryOptions::results_only();
let run = node.execute_prepared(&AllowAllAuthorizer, &open, &options)?;
let QueryResults::Solutions(rows) = run.results else {
    panic!("SELECT returns solutions")
};
assert_eq!(rows.len(), 1);

// A chosen set of crates: pass the graphs; their union is the default graph.
let names = node.prepare_query("SELECT ?name WHERE { ?s <http://schema.org/name> ?name }")?;
let run = node.execute_prepared_in_graphs(&AllowAllAuthorizer, &crates, &names, &options)?;
assert_eq!(run.statistics.result_rows, 2);

#[cfg(feature = "search")]
{
    use craqle::{SearchOptions, SearchRequest, SearchRun};

    // One budget covers the search, the final permission recheck, and hydration.
    node.flush_search_updates()?;
    let options = SearchOptions {
        timeout: Some(std::time::Duration::from_secs(30)),
        ..SearchOptions::default()
    };
    let request = SearchRequest {
        query: "sediment",
        limit: 10,
    };
    let hydrated = node.search_resources_with(
        &AllowAllAuthorizer,
        SearchRun {
            request,
            options: &options,
        },
    )?;
    assert!(!hydrated.is_empty());
    assert!(hydrated.iter().all(|hit| !hit.properties.is_empty()));
}
# Ok(())
# }
```
