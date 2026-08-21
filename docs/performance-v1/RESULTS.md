# Performance v1 results

Recorded 2026-08-21. Release-hardening source commit:
`6bb14edf6dcce9f508dd69eec838d1dd9291842d`.

Performance v1 is not fully release-complete. The current-final-binary 10M
SHACL, incremental-validation, and checked-write gate remains deferred by
explicit user request. No replacement 10M run or partial performance claim was
made during release hardening.

## Reading the tables

Unless a row says otherwise, Criterion used 10 samples, a one-second warmup,
and a five-second measurement period. p95 and p99 are nearest-rank values from
those ten samples. They commonly select the same single maximum sample and are
not strong production tail-latency estimates. Allocation counters cover the
measured operation, not RSS or process-lifetime high-water memory.

The accepted main benchmark bundle is
`.claude/evidence/20260820T204125Z-89072e57/` at `89072e57`. The corrected 1M
Auto calibration is
`.claude/evidence/20260820T223551Z-20e0514a/` at `20e0514`. Raw evidence stays
excluded; hashes and provenance are in `EVIDENCE_MANIFEST.json`.

## SPARQL selective access paths

| Fixture | Case | Auto p50/p95/p99 | Forced qv p50 | Forced source p50 | Keys, Auto/source |
| --- | --- | ---: | ---: | ---: | ---: |
| 10K | Predicate-object ASK | 0.113/0.117/0.117 ms | 0.116 ms | 0.201 ms | 1/270 |
| 10K | Predicate-object LIMIT | 0.055/0.059/0.059 ms | 0.054 ms | 0.139 ms | 1/272 |
| 1M | Predicate-object ASK | 0.263/0.267/0.267 ms | 0.281 ms | 17.612 ms | 1/31,280 |
| 1M | Predicate-object LIMIT | 0.165/0.173/0.173 ms | 0.164 ms | 16.840 ms | 1/31,287 |
| 10M historical `9d6bc52` | Predicate-object ASK | 0.304/0.323 ms | not retained | 168.810 ms | 1/312,491 |
| 10M historical `9d6bc52` | Predicate-object LIMIT | 0.190/0.203 ms | not retained | 164.364 ms | 1/312,493 |

The 10M rows are historical access-path evidence, not a current-final-binary
rerun. The trusted qv path uses linear duplicate handling; the old quadratic
union rescan fallback is not part of the current implementation.

At 1M, term-ID hash join p50 was 2.100 s versus 36.542 s for forced lateral.
The complete public result representation stayed unchanged because the faster
positional internal collector lost its advantage after compatibility
conversion.

## Native SHACL

| Fixture | Path | p50 | Allocated bytes | Peak-live delta | Data copy |
| --- | --- | ---: | ---: | ---: | ---: |
| 10K | External Rudof copy | 48.256 ms | 73,618,288 | 38,346,070 | 1,074,814 B |
| 10K | Native cached shapes | 1.206 ms | 1,437,273 | 369,840 | 0 B |
| 1M | External Rudof copy | 8.398 s | 3.260 GB | 1.175 GB | 107.9 MB |
| 1M | Native cached shapes | 0.849 s | 402 MB | 22.85 MB | 0 B |

Native cached validation was about 40.0x faster at 10K and 9.9x faster at 1M
in this paired evidence. The zero-copy claim applies to the validated data
graph; compilation still materializes the bounded shapes/import closure.

### Corrected 1M Auto/full/delta crossover

Reports and conformance normalized equally for `ForceDelta`, `ForceFull`, and
`Auto` in the accepted `20e0514` run.

| Delta | ForceDelta | ForceFull | Auto | Auto selection |
| ---: | ---: | ---: | ---: | --- |
| 1 | 0.154 ms | 19.478 ms | 0.151 ms | Delta |
| 2 | 0.239 ms | 19.277 ms | 0.241 ms | Delta |
| 5 | 0.499 ms | 19.450 ms | 0.507 ms | Delta |
| 10 | 0.946 ms | 20.083 ms | 0.971 ms | Delta |
| 25 | 2.344 ms | 20.835 ms | 2.306 ms | Delta |
| 50 | about 4.63 ms | about 22.30 ms | 4.634 ms | Delta |
| 100 | 9.113 ms | 24.421 ms | 9.206 ms | Delta |
| 250 | 23.138 ms | 30.238 ms | 23.110 ms | Delta |
| 500 | about 47.30 ms | 40.487 ms | 39.567 ms | Full |
| 1,000 | 89.907 ms | 56.485 ms | 57.681 ms | Full |

The earlier 1M result inside the main bundle is rejected because Auto selected
full too early at 100 and 250 changes. The divisor-8 calibration at
`73512e14` is rejected because it retained delta too far into large batches.
Neither is used for a release claim.

## 1M store, small bound data graphs

These final policy samples used a 1M store fixture while each bound data graph
remained small.

| Policy case | p50 | p95/p99 nearest-rank sample | Outcome |
| --- | ---: | ---: | --- |
| Disabled | 1.193 ms | 1.559 ms | Accepted, SHACL skipped |
| Unrelated Advisory | 1.572 ms | 1.891 ms | Accepted, report persisted |
| Relevant Advisory | 1.718 ms | 2.048 ms | Accepted, report persisted |
| Valid Enforce | 1.863 ms | 2.186 ms | Accepted, current report |
| Rejected Enforce | 0.381 ms | 0.408 ms | Rejected, source and status unchanged |

The qv1 PR2-to-PR3 `single_insert` p95 comparison, 139,931 ns to 142,956 ns
(+2.16%), is historical only. Its raw binary and fixture provenance are not
available, so it is not a fresh exact-HEAD result.

## Release-hardening deterministic evidence

No Criterion campaign was run for this hardening series.

- Queue scans with 0, 100, 1,000, and 10,000 binding records and no Pending
  state scanned zero queue entries. With one Pending graph they scanned one.
- A replay budget of two graphs left three of five graphs queued; restart
  settled the remaining three without duplicate reports.
- Injected failure after one successful replay left only the failed graph
  Pending and did not stop the third graph.
- Independent validation tests at 1, 2, 4, 8, and 16 writers observed the full
  requested overlap for Advisory and valid/rejected Enforce. Two writers to
  one data graph remained serialized.
- Current-status tests with 1, 10, 100, and 1,000 bindings performed exactly
  one binding-record read and two version checks per binding, with zero shape
  compilations and zero complete shapes-graph scans.
- Auto/full/delta tests cover all documented targets, path families, logical
  and qualified constraints, closed shapes, imports, `rdf:type` insert/delete,
  unrelated and shared dependencies, skew, and duplicate/no-op changes.

## Deferred gate

The reusable later-run command is `scripts/performance-v1-10m.sh`. It refuses
to start without explicit authorization, builds and hashes the benchmark
binary before timing, builds one settled deterministic fixture per process,
reuses that fixture across the timed full/incremental/write cases, enforces a
fixed cutoff, and emits no acceptance marker for a stopped or partial run.

Until that authorized run completes, no current-final-binary 10M SHACL,
incremental, or checked-write result is claimed.
