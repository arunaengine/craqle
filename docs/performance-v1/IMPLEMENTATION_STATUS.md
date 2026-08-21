# Performance v1 implementation status

Branch: `perf-v1-rocrate13-shacl`

| Work | Integrated commits | Status | Verification evidence |
| --- | --- | --- | --- |
| PR4 prepared SPARQL, plans, fast paths, join choice, and complete-result diagnostics | `49f1773` through `360153d` | Complete | Prepared/generic equivalence, fast-path differential tests, access-path and join instrumentation; accepted 10K/1M tables in `RESULTS.md` |
| PR5 Rudof parsing and portable compiled SHACL model | `40485ca` through `02ce121` | Complete | Compiler, import, cache, unsupported-component, and portable-plan tests in default/no-default SHACL builds |
| PR6 `CraqleFastV1` native engine | `e02a13b` through `8443317`, with `404df2b` limit correction | Complete | Native/Rudof normalized differential tests, bounded path/report tests, and accepted paired 10K/1M evidence |
| PR7 incremental selection and write policy | `71277da` through `f27edf4` | Complete | Full/delta parity, Enforce/Advisory/Disabled, authorization, replication, import, deletion, and lifecycle tests |
| PR8 release evidence and labels | `13e2c23` through `4d2d8db` | Code and non-benchmark matrix complete | Exact `4d2d8db` matrix passed all four check/test feature combinations, clippy, docs, and format; accepted/rejected/historical roots are separated in the manifest |
| Release hardening | `558037c` through `6bb14ed`, then the public-documentation commit | Complete | Bounded startup, lock overlap/fences, cheap status, injected settlement failures, public API surface, and expanded Auto-mode tests |

The former quadratic trusted-union fallback statement is obsolete: current qv
duplicate handling is linear. Completed PR4-PR8 work is not listed as Pending.

The final-binary 10M SHACL, incremental-validation, and checked-write
performance gate is deferred by explicit user request. This is the only open
Performance v1 release-evidence item and prevents a fully release-complete
label. Historical 10M SPARQL access-path rows remain clearly labeled as such.
