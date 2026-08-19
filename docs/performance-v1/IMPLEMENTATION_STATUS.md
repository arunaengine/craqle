# Performance v1 implementation status

Integration branch: `perf-v1-rocrate13-shacl`

Starting commit: `6412e699ac86648e4b02b19b8953ee4a78b3dce0`

| Task | Model | Branch | Status | Changed files | Reviewer | Tests | Benchmark result | Open risks | Follow-up |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Initial inspection and dependency verification | gpt 5.6 sol | `perf-v1-rocrate13-shacl` | Complete | This ledger only | gpt 5.6 sol | `cargo test --locked` | 1,000-graph query matrix recorded; selective cases improve 16–35x with the existing optimizer | Absolute timings are debug-build, small-host baselines only | Preserve current wins and add release-mode repeatable measurements in PR 0 |
| PR 0A deterministic corpus, fixtures, and baseline documentation | luna max | `perf-v1-pr0-fixtures` | Assigned | Pending | gpt 5.6 sol | Pending | Pending | Corpus scale and determinism must not require committed large fixtures | Worker implementation, lead review, focused rerun |
| PR 0B measurement harness, counters, and query differential coverage | terra max | `perf-v1-pr0-instrumentation` | Assigned | Pending | gpt 5.6 sol | Pending | Pending | Instrumentation must remain low-overhead and must not change query semantics | Worker implementation, lead review, focused rerun |
| PR 1 dependency pins and RO-Crate 1.3 | luna max and terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Context preservation and version agreement need semantic review | Start after PR 0 acceptance |
| PR 2 shared RDF read view, cursors, cancellation, counters, and delta view | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Lock lifetimes, cancellation propagation, and fallback correctness | Start after PR 1 acceptance |
| PR 3 persistent sorted RDF read indexes | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Atomic liveness transitions, crash state, rebuild, and fallback | Start after PR 2 acceptance |
| PR 4 prepared SPARQL, sinks, fast paths, join choice, and plans | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | SPARQL semantics and early-stop proof | Start after PR 3 acceptance |
| PR 5 Rudof parser integration and compiled SHACL model | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Term conversion, unsupported components, and cache keys | Start after PR 4 acceptance |
| PR 6 CraqleFastV1 SHACL engine | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Constraint semantics, bounded paths, and report equivalence | Start after PR 5 acceptance |
| PR 7 incremental SHACL and write policy | terra max | Pending | Pending | Pending | gpt 5.6 sol | Pending | Pending | Pre-commit isolation and replicated convergence | Start after PR 6 acceptance |
| PR 8 release checks, documentation, and final correction | gpt 5.6 sol | `perf-v1-rocrate13-shacl` | Pending | Pending | gpt 5.6 sol | Pending | Pending | Full matrix and performance gates must use one host and one run | Start after PR 7 acceptance |
