# Changelog

All notable changes to Craqle are documented here.

## 0.2.0-rc.1

### Added

- Persistent qv query indexes and prepared SPARQL execution.
- Native bounded `CraqleFastV1` SHACL compilation, validation, policy binding,
  status, and bounded recovery APIs.
- RO-Crate 1.1, 1.2, and 1.3 import and export handling.
- Stable high-level public error categories and an authoritative disk-format
  marker.
- A bounded, result-checked SPARQL comparison harness pinned to Oxigraph
  `0.5.9`; it rejects unauthorized 10M inputs.

### Changed

- Craqle now publishes a defined 0.2.x source and authoritative disk-data
  compatibility contract.
- Vendored dependency source trees were replaced by released registry crates
  where available and exact Git revisions otherwise.

### Fixed

- Query-index duplicate handling is linear and selective query paths use
  persistent access indexes.
- SHACL restart recovery is bounded, independent graph validations overlap,
  status reads avoid compilation, and post-commit settlement failures remain
  durably Pending.

### Performance

- Accepted 1M selective SPARQL, joins, native SHACL, incremental validation,
  and small-bound-graph checked-write baselines are retained.
- Current-final-binary 10M SHACL, incremental-validation, and checked-write
  evidence remains deferred until explicitly authorized.
- The initial direct Oxigraph diagnostic verifies the focused query result
  matrix at 10K and 1M but uses an in-memory Oxigraph store, so the
  same-durability comparison gate remains open.

### Compatibility

- Rust 1.91 is the measured all-feature MSRV.
- Authoritative disk format `1.0` fails closed for missing, malformed, or
  unknown future format markers.

### Known limits

- `CraqleFastV1` is a bounded SHACL profile; full SHACL Core is not claimed.
- SHACL-SPARQL, SHACL-JS, SHACL-AF, remote imports, RDF-star, and other listed
  unsupported forms fail explicitly.
- The release candidate still uses exact Git dependencies and is not yet
  eligible for a crates.io publication.
### Migration from 0.1.x

The first 0.2 deployment uses a new empty database. Craqle does not silently
adopt a non-empty unmarked 0.1 store; export authoritative data with the old
release and import it into a new 0.2 store.
