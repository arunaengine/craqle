<!-- Runs and interprets the isolated Fluree reference adapter. -->
<!-- Copyright (c) 2026 ArunaStorage Team @ JLU Giessen -->
<!-- SPDX-License-Identifier: MIT -->

# Fluree local adapter

This adapter runs against the pinned Fluree checkout at
`.claude/query-performance/reference/fluree`, revision
`82dbcec3e435d6ed1d45bc0ed929432323b6b201`. It is not part of Craqle's Cargo
targets or dependency graph.

Copy the source into the reference package, then build and run it from that
checkout:

```bash
cp benches/fluree_adapter.rs \
  .claude/query-performance/reference/fluree/fluree-db-api/examples/craqle_compare.rs
python3 -B scripts/guard.py .claude/query-performance/reference/fluree \
  .claude/query-performance/fluree-run-resources.jsonl \
  env CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_STRIP=false \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  cargo +stable run --locked --release -p fluree-db-api --example craqle_compare
```

The Fluree workspace and any uncached crates must already be available. Cargo
may otherwise need registry network access. Run builds under the same resource
guard as Craqle and retain each checkout's own target directory.
The explicit stable override matches Craqle's comparison compiler; record
its exact version, since Fluree's checkout otherwise selects Rust 1.97.0.
The release overrides match Craqle's default compiler profile instead of
Fluree's default LTO, stripping, and single code generation unit.

The following variables select the fixed synthetic workload:

```text
FLUREE_SUBJECTS=32
FLUREE_MULTIPLICITY=2
FLUREE_SAMPLES=5
FLUREE_BM25_LIMIT=10
FLUREE_BM25_QUERY="common rare"
```

Startup, fixture insertion, binary reindexing, BM25 construction, and index
loading are reported outside query timings. The SPARQL record includes the
complete sorted result bag, preserving duplicate rows. The BM25 record includes
every positive-score candidate in Fluree rank order, score bits, and a separate
top-k result.

The SPARQL bag can be compared only after another engine loads the same RDF
fixture and runs the same query. Fluree BM25 uses English stopword filtering,
Snowball stemming, and f64 scoring. Craqle uses a Tantivy tokenizer pipeline,
f32 scores, quantized field norms, graph and subject identity text, active
generation filtering, and authorization checks. The BM25 numbers therefore
measure Fluree's local candidate scorer and do not establish score, ranking,
visibility, authorization, or service parity with Craqle.
