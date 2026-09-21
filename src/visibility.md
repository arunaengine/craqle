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
