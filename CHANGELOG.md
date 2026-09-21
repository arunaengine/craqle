<!-- Records released behavior and upgrade requirements. -->
<!-- Copyright (c) 2026 ArunaStorage Team @ JLU Giessen -->
<!-- SPDX-License-Identifier: MIT -->

# Changelog

All notable changes to Craqle are documented here.

## 0.3.0 - Unreleased

### Changed

- Integrates Irokle 0.3.0 at revision `fd29484e9281524efa692f59025ac6cca2443da8`.
- Requires Rust 1.97.1 or newer. Craqle's authoritative RDF disk format remains `1.0`.
- Background maintenance retries failed search entries and rebuilds uncovered query
  indexes. Rebuilds prepare an inactive index and replay concurrent source changes
  before a short publication fence.
- Dropping a node joins its maintenance worker; an active storage or search call
  must finish before shutdown completes.
- Query statistics, explain, and analyze keep only timings, result counts, the
  query form, and a fingerprint of the query text unless the authorizer reads every
  graph. The physical plan, join choices, fast-path kind, access paths, estimates,
  and store counters can depend on unreadable graphs, so they are withheld and the
  plan root reports `QueryPhysicalOperator::Withheld`. `details_withheld` on
  `QueryPlan` and `QueryExecutionStatistics` tells a withheld zero from a measured
  one. `Authorizer::reads_all` defaults to `false`; `AllowAllAuthorizer` returns `true`.
- Search fails with `CorruptDerivedData` when a live index document has missing or
  malformed metadata, and queues a reindex of its graph, instead of skipping it.
  Damage without a readable graph scope, or beyond 64 pending graphs, queues a
  whole search rebuild. A repair stays pending until the store accepts it, and a
  late report from an older search view cannot replace a current one. A whole
  rebuild ends by deleting every document outside the published and staged
  generations, so damaged records with no usable scope or key are removed.
- A query over exactly one explicit graph reads that graph's own index range,
  including counts from its per-graph counters, instead of scanning the union of all
  graphs and discarding rows from others. Results are unchanged. Larger graph lists
  and policy-based default unions still use the union scan with visibility checks.
- Default-union queries, statistics, prepared execution, explain, and analyze fail
  with the store error when a graph policy cannot be read. Earlier versions hid
  that graph and could return fewer rows, a smaller count, or a false `ASK`.
  A missing or denying policy still hides the graph.

### Added

- Fixed-target search flush receipts, request cancellation, and explicit timed
  shutdown that retains ownership of unfinished maintenance.
- Mutation receipts with separate acceptance, durability, and repair outcomes.
  Keyed mutations first return an admission ticket, then apply when that ticket
  is supplied; expired tickets cannot silently repeat a mutation.
- Authorized graph reconciliation from retained history or a verified healthy
  snapshot, with a durable backup and an audit of the replacement.
- Explicit process and store memory reservations shared by live stores.
- `QueryOptions::results_only()` runs a query without per-operator statistics.
  `QueryOptions::default()` still collects them for compatibility.
- `CraqleNode::search_with_options` accepts `SearchOptions` with cancellation and a
  timeout. Either stops the search with an error instead of returning partial hits.
  `search_graphs_with` and `search_resources_with` apply the same options to
  graph-scoped and hydrated search. One budget covers setup, scoring, the final
  permission recheck, and hydration, and an ended budget fails even when nothing
  matches. Checks are cooperative, so this is not a strict wall-clock bound.

### Upgrading from 0.2

- Query and update limits cover storage reads and materialized results while
  preserving supported SPARQL operators. Generic evaluator buffers are not fully
  accounted for, and cancellation inside those operators remains cooperative.
- Search-disabled builds retain index repair debt and report search flush as
  unsupported. Reopening with search enabled repairs that debt before coverage
  can be certified.

- Applications that depend directly on Irokle must use the same revision as Craqle,
  or use the `craqle::irokle` re-export so both libraries share identical types.
- Upgrade replication peers together: Irokle 0.3 uses the `irokle/sync/2` protocol.
- Back up Irokle's Fjall database before its first open with this version. Irokle
  upgrades schema 1 to schema 2 in place and resumes an interrupted migration.
  Older Irokle binaries reject schema 2; rollback requires the pre-upgrade backup.
- Derived-index repair preserves surviving RDF source state. It cannot reconstruct
  source events lost by older bugs without an authoritative history or healthy replica.

## 0.2.0 - 2026-08-22

### Added

- Persistent qv query indexes and prepared SPARQL execution.
- Native Craqle SHACL Core Subset v1 compilation, validation, policy binding,
  status, and bounded recovery APIs.
- RO-Crate 1.1, 1.2, and 1.3 import and export handling.
- Stable high-level public error categories and an authoritative disk-format
  marker.
- Durable graph tombstones, deterministic tagged policy events, a replication
  rejection ledger, and audited topic-cursor repair.
- Production `QueryLimits`, `UpdateLimits`, and `RoCrateImportLimits`.

### Changed

- `SyncAll` is the default local persistence mode. `Buffer` is available only
  through an explicit caller choice.
- Explicit graph query, prepared execution, explain, analyze, statistics, and
  SPARQL Update entry points require an `Authorizer` and fail the whole request
  if any selected graph is unreadable or missing.
- Public trusted-import, unchecked-write, local-update, closure-visibility, and
  physical-path bypasses were removed or confined to crate-private tests.
- Structural RO-Crate checks and SHACL enforcement use separate `WriteChecks`;
  one option cannot disable both.
- The SHACL API uses `ShaclProfile::CoreSubsetV1`, `ShaclWritePolicy`,
  `ShaclEvaluationMode`, domain-prefixed option/report names, and an independent
  `ShaclBlockingSeverity` write threshold.
- Durable source and qv keyspaces are read authorities. Graph-generation-tagged
  caches are bounded, and independent graph commits no longer hold one global
  in-memory index lock across Fjall commit or sync.
- Ordinary RO-Crate import uses the prepared-document path, bounded encoded
  sorted diffs, and atomic data-plus-render-hint commits. Export cursors are
  versioned, graph-bound, version-bound, and checksummed.
- Distribution is the Linux-only `v0.2.0` Git tag with exact Git dependency
  revisions. Craqle 0.2 is not a crates.io package.

### Fixed

- Degraded `COUNT(DISTINCT ?o)` no longer assumes object adjacency in GSPO
  order. Every count cursor declares subject, object, or no grouping; unmatched
  grouping uses a `max_hash_entries`-bounded exact set and never approximates.
- SPARQL `DELETE/INSERT WHERE`, `WITH`, `USING`, graph variables, and write
  targets use authorization-aware visibility. Private bindings cannot be copied
  into a writable public graph.
- Writes to tombstoned graph IDs return `GraphDeleted`; replicated writes and
  unauthorized/malformed/unsupported/corrupt records are durably rejected
  before cursor advance, with idempotent `seen_count` updates.
- Remote policy changes deny by default, require `RemotePolicyAuthorizer`, and
  converge by total `PolicyTag` order.
- Corrupt topic cursors fail with `CorruptAuthoritativeData` and never reset to
  zero.
- Query and update limits are enforced during evaluation and materialization;
  a breach returns one error rather than a partial result.
- RO-Crate input bounds, calendar-valid dates, atomic render hints, and typed
  invalid export cursor failures are enforced.
- Recursive SHACL shapes and RDF-star terms fail explicitly as unsupported.
- Search calls return `Unsupported` when the `search` feature is disabled.
- Stale markdown signatures were replaced by a compiled end-to-end API example.

### Performance

- The retained 10K Craqle/Oxigraph diagnostic reported roughly 2.5x to 4.9x
  typical gains, roughly 1.7x for one-or-more path, roughly 3.2x for exact
  count, and roughly 4.8x for triangle (2.54 ms versus 12.28 ms). The corrected
  triangle ratio is about 0.207; the incompatible earlier value was an
  arithmetic error.
- Retained internal evidence measured EXISTS and NOT EXISTS count fast paths at
  roughly 25x and 28x faster than generic evaluation.
- Retained native cached SHACL validation was roughly 40x faster at 10K and
  roughly 9.9x faster at 1M than the external-copy path.
- The post-count-change 1M Craqle/Oxigraph comparison was not rerun. Short 10K
  measurements are diagnostic, not production-tail results. No new benchmark
  was required or run for this release.

### Compatibility

- v0.2.0 begins the documented 0.2.x compatibility line.
- Linux is the only supported platform. Rust 1.91 is the measured all-feature
  MSRV.
- Authoritative disk format `1.0` fails closed for missing, malformed, or
  unknown future format markers.

### Known limits

- Craqle SHACL Core Subset v1 is bounded; full SHACL Core is not claimed.
- SHACL-SPARQL, SHACL-JS, SHACL-AF, remote imports, RDF-star, and other listed
  unsupported forms fail explicitly.
- Recursive shapes are unsupported in 0.2.

### Migration from 0.1.x

The first 0.2 deployment uses a new empty database. Craqle does not silently
adopt a non-empty unmarked 0.1 store; export authoritative data with the old
release and import it into a new 0.2 store.
