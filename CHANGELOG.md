# Changelog

All notable changes to Craqle are documented here.

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
