# Dependency compatibility provenance

This note records the evidence-approved compatibility adjustment for PR1. It
is not a silent version move: the release pins, source snapshots, and local
patches below are deliberate, reviewable overrides and must be removed when
the corresponding upstream releases provide the same compatibility.

## Release sources and hashes

| Snapshot | Original source | Immutable release identity | Release/package hash |
| --- | --- | --- | --- |
| `vendor/ro-crate-rs-0.6.0` | `https://github.com/intbio-ncl/ro-crate-rs.git` | tag `v0.6.0`; commit `855b8038e7170028bd2c3f8a2425a3abc73e2b93` | vendor content digest `da8122dbe19ade2031463c68384e1e2b31200f7a7108358862fa2a05df82cbeb` |
| `vendor/shacl-0.3.8` | crates.io package [`shacl 0.3.8`](https://crates.io/crates/shacl/0.3.8); repository `https://github.com/rudof-project/rudof` | package VCS commit `cad38405d2b99cdd256b6e2c0bb2fb405c1babf8` (`.cargo_vcs_info.json`); release tag `0.3.8` points to that commit | crates.io package SHA-256 `63e08ab108bba3320bd8f19659cf4cefa7b970f7220b4e794d3b6a6acd86f336`; vendor content digest `755a70ac36260bb3959fb517146f2d922594c290d3dd6bb64cb33e72a485b17e` |

The lockfile records the published Rudof package checksums used by this
snapshot: `rudof_config` `3bcc1c906f8782d74a88d745343a7945f1d814f3a185df44bec4992e39e5e417`,
`rudof_iri` `bb558a7b3208da242c5ace51453f72a1bb4226e07463adcd222e89e0f43f8ce5`,
and `rudof_rdf` `81c5abbee907488e11b39054e804b48fd6e8a23293fcac0c5d52fe33cf31e874`.

The RO-Crate snapshot retains the upstream Apache-2.0 `LICENSE` and
`README.md`. The SHACL snapshot retains the upstream `README.md`, exact
`LICENSE-MIT` (SHA-256 `2f74eb8a28ae8e274e24fcc508667353a0d0971f261c4f23cf6cb7135be53dd5`),
and exact `LICENSE-APACHE` (SHA-256
`0a949a838477a61f93aa1b6fe0119cec4364db31b92d432622831a1cb1a98377`) from
the `0.3.8` release tree. It also retains the crates.io
`.cargo_vcs_info.json` and `Cargo.toml.orig` provenance files byte-for-byte;
the normalized vendor manifest keeps the published `MIT OR Apache-2.0`
attribution.

The vendor-content digests above are reproducible from the repository root
with this exact path-sorted command (the output is a hash of the sorted
per-file SHA-256 records, including relative paths):

```text
for snapshot in ro-crate-rs-0.6.0 shacl-0.3.8; do
  (cd "vendor/$snapshot" && find . -type f -print0 | sort -z | xargs -0 sha256sum) | sha256sum
done
```

## Why the overrides are required

The two defects were reproduced and reviewed before this snapshot was made.
Cargo-only resolution cannot repair either one: feature unification can turn
on an `oxrdf` feature in a dependency graph, but it cannot add a wildcard arm
or change conversion return types in the already-published RO-Crate source;
likewise, Cargo feature selection cannot gate imports in published SHACL
source that are unconditional or make its non-optional dependency optional.
The local patches are therefore required while retaining the requested exact
release identities; no newer version was selected.

* RO-Crate v0.6.0's upstream `src/ro_crate/rdf/rdf_io.rs` had exhaustive
  `oxrdf::Term` matches at `push_outgoing_ref` (lines 340-346) and
  `term_to_entity_value` (lines 780-850). Craqle's RDF ecosystem can unify
  `oxrdf/rdf-12` through another dependency, adding `Term::Triple`; the
  release therefore could not compile cleanly in that feature-unified graph
  and had no typed conversion failure for RDF-star terms. The reviewed patch
  adds `RdfError::UnsupportedRdfStarTerm` in `error.rs`, makes conversion
  fallible, propagates it at both public conversion boundaries, and rejects
  unsupported terms explicitly. The wildcard matches are intentionally
  feature-agnostic because a dependency cannot observe a downstream feature
  unification.
* shacl 0.3.8's published manifest enabled `default = ["sparql"]` and kept
  the SPARQL service dependency non-optional. Its native validation path also
  imported SPARQL-only types unconditionally, so a consumer selecting
  `default-features = false` could not obtain a native-only build. The exact
  probe failed with 11 `E0432` unresolved imports, including
  `OxigraphEndpointError`, `sparql_service::RdfDataError`, `SparqlEngine`,
  `RdfData`, `BasicSparqlValidator`, and `escape_sparql_string`. The reviewed
  compatibility patch makes `sparql_service` optional, adds the dependency
  feature edge, gates the SPARQL imports/implementations, and adds the native
  `OxigraphInMemory` path. It leaves each native constraint algorithm
  unchanged. Upstream Native mode's intentional `BasicSparql` no-result
  fallback when `sparql` is disabled is preserved; Craqle must explicitly
  detect/report `sh:sparql` during compilation and must never infer conformity
  from that fallback.

## Reviewed patch files

RO-Crate has exactly these source changes (44 additions, 23 removals):

* `src/ro_crate/rdf/error.rs`
* `src/ro_crate/rdf/rdf_io.rs`

SHACL has the normalized manifest plus exactly these 14 source changes (77
additions, 11 removals):

* `src/validator/constraints/core/other/closed.rs`
* `src/validator/constraints/core/property_pair/{disjoint,equals,less_than,less_than_or_equals}.rs`
* `src/validator/constraints/core/string_based/pattern.rs`
* `src/validator/constraints/core/value/class.rs`
* `src/validator/constraints/mod.rs`
* `src/validator/constraints/sparql/{basic_validator,mod}.rs`
* `src/validator/engine/mod.rs`
* `src/validator/error.rs`
* `src/validator/processor/graph.rs`
* `src/validator/store/graph.rs`

## Source-copy policy

The snapshots were copied from the exact reviewed release trees. They retain
the complete build library source trees and all RO-Crate JSON-LD resources.
The RO-Crate README and license are retained. The SHACL README, both exact
release license files, `.cargo_vcs_info.json`, and `Cargo.toml.orig` are
retained. Upstream CLI sources, tests, examples, benches, docs, CI files,
generated lockfiles, repository VCS working-tree metadata, target directories,
and disposable probes were not copied. The three SHACL `src/*/test.rs`
modules remain because the upstream library module tree references them even
when their contents are cfg-gated. The RO-Crate nested workspace was reduced
to `members = []` and omitted targets/dev-dependencies were removed solely so
the minimal vendor package remains a buildable dependency without the omitted
CLI/examples/benches. The SHACL targets/examples/tests were omitted only after
its normalized manifest disabled their automatic discovery; the normal library
target remains complete.

## Cargo wiring and verification

`Cargo.toml` keeps the visible `rocraters` git URL and immutable `tag =
"v0.6.0"`, with `features = ["rdf"]`. A narrowly scoped git-source patch
selects the local RO-Crate snapshot; a crates.io patch selects the local
shacl 0.3.8 snapshot. The direct optional `shacl`, `rudof_rdf`, and `rudof_iri`
dependencies are exact `=0.3.8`, optional, and all use `default-features = false`.
`shacl-core` is opt-in and the Craqle default remains exactly `search`.
Because a path patch takes ownership of the selected package, Cargo.lock
intentionally has no registry checksum or git `source` field for the patched
RO-Crate and SHACL package entries; the visible release declarations, exact
upstream identities, and content hashes above are the source-of-truth proof.

Use the normal disk-backed target directory and at most eight build jobs:

```text
CARGO_BUILD_JOBS=8 cargo fmt --all -- --check
CARGO_BUILD_JOBS=8 cargo check --locked
CARGO_BUILD_JOBS=8 cargo check --locked --no-default-features
CARGO_BUILD_JOBS=8 cargo check --locked --features shacl-core
CARGO_BUILD_JOBS=8 cargo check --locked --no-default-features --features shacl-core
CARGO_BUILD_JOBS=8 cargo tree --locked --features shacl-core -e features -i shacl
CARGO_BUILD_JOBS=8 cargo tree --locked --features shacl-core -e features -i rudof_rdf
CARGO_BUILD_JOBS=8 cargo tree --locked --features shacl-core -i sparql_service
git diff --check
```

The feature-tree review must show no `shacl/sparql`, `rudof_rdf/sparql`, or
`sparql_service` in the native-only configuration; the inverse tree query is
expected to report `sparql_service` as not found when it is not selected.
The current locked tree reports `shacl v0.3.8` and `rudof_rdf v0.3.8` without
those SPARQL features, and the inverse query exits 101 with
`package ID specification \`sparql_service\` did not match any packages`.

The working-tree `git diff --check` is clean. The staged full check reports 33
upstream-preserved whitespace findings only: 28 in the RO-Crate `README.md`,
one in `src/ro_crate/context.rs`, two in `src/ro_crate/modify.rs`, and two in
the SHACL `README.md` (including their upstream blank lines at EOF). No
compatibility patch line or documentation line is implicated. This is a
provenance-preserving exception; the scoped staged check for this change set
is:

```text
git diff --check -- . \
  ':(exclude)vendor/ro-crate-rs-0.6.0' \
  ':(exclude)vendor/shacl-0.3.8'
```

## Removal / rollback condition

To update, first verify a newer upstream source against the two base identities
and the per-file source diff, then remove the corresponding local `[patch]`
override and snapshot, regenerate `Cargo.lock`, and rerun the full locked
matrix plus feature-tree checks against the published package. Do not update
solely because a newer version exists. To roll back, restore the prior commit
or restore the exact snapshot, patch, and lockfile together. Removal is safe
only after upstream releases demonstrably contain the reviewed RO-Crate RDF-
star rejection/fallible propagation and SHACL default-off native build.
Until then, keep the immutable tag declaration, exact lockfile versions, and
this provenance note together.

No performance claim is made by this dependency compatibility adjustment;
verification here is compilation and dependency-resolution proof only.
