# Performance v1 baseline

This is the pre-PR0 smoke baseline for the `perf-v1-rocrate13-shacl`
integration branch. It is evidence for repeatable comparisons, not a release
claim. The measurements below were captured on the same host and should be
repeated after the deterministic corpus and instrumentation land.

## Provenance and current state

- Source baseline: `6412e699ac86648e4b02b19b8953ee4a78b3dce0`.
- Lead ledger at capture: `11bdb5f00b889a6a80c421908b80426f6597f55b`.
- Current package: `craqle` `0.1.1`, Rust edition `2024`.
- Current default feature set: `default = ["search"]`; `search` enables
  Tantivy `0.26.1`. The optional `iroh` feature enables Iroh support through
  Irokle. No Rudof or SHACL dependency has landed in this baseline.
- Current locked Git dependencies include Irokle `0.1.3` at
  `ebd4abd142f66816a16492c615525809f6c8b663` and `ro-crate-rs` `0.5.1` at
  `169aab72ae55773c327eb2d713fd503a36797539`.
- Later dependency targets were verified for PR1: `ro-crate-rs` `v0.6.0` at
  `855b8038e7170028bd2c3f8a2425a3abc73e2b93`, Rudof tag `0.3.8` at
  `cad38405d2b99cdd256b6e2c0bb2fb405c1babf8`, and crates.io `shacl 0.3.8`
  plus `rudof_rdf 0.3.8`. The latter two default to `sparql`; PR1 must use
  `default-features = false` for the opt-in comparison dependency set.

Host and toolchain capture supplied for this baseline:

```text
Linux 7.1.8 x86_64
AMD Ryzen 9 7945HX, 16 cores / 32 threads
62 GiB RAM
rustc 1.97.1 (8bab26f4f 2026-07-14)
cargo 1.97.1 (c980f4866 2026-06-30)
```

The capture commands are:

```sh
git rev-parse HEAD
git show --no-patch --format=fuller HEAD
rustc -Vv
cargo -V
uname -a
lscpu
free -h
cargo metadata --locked --no-deps --format-version 1
cargo tree --locked --depth 1
```

## Correctness and smoke commands

The untouched active suite passed before PR0:

```sh
cargo test --locked
```

The existing debug query smoke passed with 1,000 configured graphs, two warm
samples, and concurrency two:

```sh
CRAQLE_MATRIX_GRAPH_COUNT=1000 CRAQLE_MATRIX_SAMPLES=2 \
CRAQLE_MATRIX_CONCURRENCY=2 \
cargo test --locked --test perf_query_matrix \
  query_plan_matrix_before_after -- --ignored --nocapture
```

Its fixture load was `843.406222ms`; the p50 query timings were:

| Shape | Optimizer off | Optimizer on | Rows |
| --- | ---: | ---: | ---: |
| S01 ASK | 448.981us | 410.329us | 1 |
| S02 LIMIT25 | 1.136872ms | 1.192234ms | 25 |
| S03 selective-last BGP | 14.886215ms | 564.408us | 1 |
| S04 selective-first | 489.377us | 628.408us | 1 |
| S05 OPTIONAL | 1.716838ms | 1.861569ms | 25 |
| S06 filter-eq | 21.038196ms | 601.107us | 1 |
| S07 UNION | 1.983318ms | 2.044552ms | 50 |
| S08 graph-var | 1.38685ms | 1.265132ms | 25 |
| S09 EXISTS | 35.60399ms | 2.091771ms | 25 |
| S10 NOT EXISTS | 34.00379ms | 2.081753ms | 25 |
| S11 chain | 5.904979ms | 5.191221ms | 100 |
| S12 anchored worst-order | 15.046225ms | 771.817us | 1 |
| S13 DISTINCT | 1.084463ms | 1.145778ms | 25 |
| S14 order selective | 656.601us | 727.363us | 1 |
| S15 order corpus | 78.35113ms | 79.259824ms | 10 |
| S16 contains | 20.783258ms | 20.334638ms | 2 |

The supplied release smoke also passed:

```sh
CRAQLE_MATRIX_GRAPH_COUNT=2000 CRAQLE_MATRIX_SAMPLES=5 \
CRAQLE_MATRIX_CONCURRENCY=4 \
cargo test --release --locked --test perf_query_matrix \
  query_plan_matrix_before_after -- --ignored --nocapture
```

It loaded 2,000 graphs / 1,960 live graphs in `204.107551ms`. The p50 query
timings were:

| Shape | Optimizer off | Optimizer on | Rows |
| --- | ---: | ---: | ---: |
| ASK | 25.628us | 23.424us | 1 |
| LIMIT25 | 126.536us | 129.642us | 25 |
| selective-last BGP | 4.982801ms | 34.224us | 1 |
| selective-first | 27.732us | 33.632us | 1 |
| property-ish OPTIONAL | 242.152us | 223.297us | 25 |
| filter-eq | 5.390092ms | 27.522us | 1 |
| UNION50 | 237.934us | 241.861us | 50 |
| graph-var25 | 148.156us | 139.150us | 25 |
| EXISTS | 7.852528ms | 269.213us | 25 |
| NOT EXISTS | 10.268138ms | 317.132us | 25 |
| chain100 | 884.231us | 769.997us | 100 |
| anchored worst order | 5.298771ms | 65.462us | 1 |
| DISTINCT25 | 120.835us | 126.045us | 25 |
| ordered selective | 42.249us | 48.140us | 1 |
| ordered corpus | 18.851651ms | 18.278481ms | 10 |
| contains scan | 6.547322ms | 7.125381ms | 2 |

These are debug and small release smoke runs over the existing matrix, not the
final deterministic corpus and not an SLO or release-performance claim.

## Deterministic PR 0 harness

PR 0 adds a fixed-seed streaming corpus generator for 10,000, 1,000,000, and
10,000,000 quads; 1, 32, and 1,000 graphs; and 0%, 25%, and 90% cross-graph
duplicate rates. The iterator has constant-sized state, large corpora are
generated rather than committed, and focused tests pin exact counts, graph
coverage, duplicate placement, property-star locality, chain locality, hidden
graphs, and orphan metadata.

The final lead-reviewed 10,000-quad, 32-graph, 25%-duplicate SPARQL smoke used:

```sh
CRAQLE_BENCH_WARMUP_SECS=1 CRAQLE_BENCH_MEASUREMENT_SECS=1 \
  cargo bench --locked --bench sparql_hot_path
```

It recorded 10 rows for the bounded SELECT, 789 rows for COUNT, one unique
property-star subject, and one unique join subject in both written orders.
The observed intervals were:

| Case | Criterion interval |
| --- | ---: |
| Bound ASK hit | 96.222-98.532 us |
| Bound ASK miss | 93.711-96.587 us |
| SELECT LIMIT 10 | 168.45-210.26 us |
| Exact COUNT | 1.0523-1.2011 ms |
| Same-subject property star | 135.74-142.98 us |
| Rare-to-common join | 115.03-120.99 us |
| Common-to-rare join | 117.24-120.97 us |

These timings still measure fully collected public `QueryResults`. They do not
claim time to first row or bounded underlying reads.

The corresponding lead-run memory smoke used:

```sh
CRAQLE_BENCH_WARMUP_SECS=1 CRAQLE_BENCH_MEASUREMENT_SECS=1 \
  cargo bench --locked --bench read_path_memory
```

The database occupied 67,869,510 bytes. Linux `/proc/self/status` samples were:

| Phase/case | Result | VmRSS bytes | VmHWM bytes |
| --- | ---: | ---: | ---: |
| Before fixture | - | 6,201,344 | 6,803,456 |
| After fixture | - | 261,709,824 | 268,898,304 |
| Bound ASK hit | Boolean / 1 | 261,726,208 | 268,898,304 |
| Fixed predicate-object LIMIT 10 | Solutions / 10 | 261,726,208 | 268,898,304 |
| Broad visible union | Solutions / 7,496 | 261,963,776 | 268,898,304 |
| Duplicate-heavy union | Solutions / 474 | 261,971,968 | 268,898,304 |

VmRSS is coarse process-wide state and VmHWM is cumulative for the process;
neither is a query-local allocator measurement. A no-default-feature smoke with
`CRAQLE_MEMORY_BROAD_SCAN=0` also passed and explicitly reported the broad scan
as skipped.

## Required metric capture

No pre-instrumentation counters are inferred from elapsed time. A value is
listed only where the supplied run recorded it.

| Required metric | Baseline value | Status / capture note |
| --- | --- | --- |
| Wall time | Debug fixture load `843.406222ms`; release fixture load `204.107551ms`; deterministic query intervals above | Partial; completed-result time only |
| First row / violation | — | Pending instrumentation |
| Candidate quads | — | Pending instrumentation |
| Matching quads | — | Pending instrumentation |
| Index seeks | — | Pending instrumentation |
| Graphs considered | — | Pending instrumentation; configured corpus graph counts are not a read-work counter |
| Terms decoded | — | Pending instrumentation |
| Rows emitted | Per-case counts in the timing tables; memory cases report 1, 10, 7,496, and 474 | Captured for completed results |
| Shape/focus pairs | — | Pending SHACL instrumentation |
| Path edges | — | Pending instrumentation |
| Allocated bytes | — | No allocator instrumentation; encoded term payload bytes are not presented as allocations |
| Peak RSS | Process VmHWM `268,898,304` bytes in the lead memory smoke | Coarse process-wide measurement |
| Write p50 / p95 / p99 | — | Pending write harness/instrumentation |
| Database bytes | `67,869,510` bytes for the final lead 10K memory smoke | Captured from the generated database directory |

## Known correctness and comparison gaps

The current union-default path has an observed duplicate-row baseline defect
when duplicate triple payloads are present across graphs. This is recorded as a
known pre-PR0 defect; the intended union-default duplicate regression remains
ignored until PR3 changes the read/index path. PR0 does not alter production
query semantics.

The external Rudof SHACL comparison required two exact-release compatibility
adjustments before it could run: Rudof's RDF dependencies enable `oxrdf` RDF
1.2 terms, exposing unhandled `Term::Triple` cases in `ro-crate-rs v0.6.0`;
separately, published `shacl 0.3.8` does not compile with its default `sparql`
feature disabled because several validator imports and implementations are not
consistently feature-gated. PR 1 retains the exact released versions, applies
the reviewed local source adjustments documented in
`DEPENDENCY_COMPATIBILITY.md`, and proves the `sparql` feature and
`sparql_service` package are absent.

## External Rudof SHACL baseline

After those dependency-only compatibility adjustments, the lead-reviewed
10,000-quad, 32-graph, 25%-duplicate comparison used:

```sh
CRAQLE_BENCH_QUADS=10000 CRAQLE_BENCH_GRAPHS=32 \
CRAQLE_BENCH_DUPLICATE_PERCENT=25 CRAQLE_BENCH_WARMUP_SECS=1 \
CRAQLE_BENCH_MEASUREMENT_SECS=1 \
  cargo bench --locked --features shacl-core --bench shacl_external_baseline
```

The untimed pass exported and copied 5,971 visible unique triples into Rudof's
in-memory graph, then produced exactly 291 expected violations. It recorded
36.005 ms for Craqle export plus Rudof copy, 0.293 ms for shapes parsing and IR
compilation, 4.936 ms for completed native validation, and 42.633 ms total.
The Criterion intervals were:

| Phase | Criterion interval |
| --- | ---: |
| Visible export and Rudof copy | 36.352-37.661 ms |
| Shapes parse and IR compile | 82.751-84.971 us |
| Retained-copy native validation | 2.6805-2.8187 ms |
| Full export, copy, compile, and validation | 43.961-44.418 ms |

Linux process samples were 6,463,488 bytes VmRSS / 7,634,944 bytes VmHWM
before fixture construction, 326,369,280 / 335,142,912 after the fixture, and
328,888,320 / 335,142,912 after validation. These are process-wide samples,
not allocation attribution. Rudof Native returns only a completed report, so
4.936 ms is explicitly a completed-validation duration and an upper bound for
time to first violation, not a true first-violation measurement. This full
data-graph copy exists only in the comparison benchmark and is forbidden from
the later production validation and write paths.

## PR 0 verification

After every worker diff was read and the lead corrections landed, the
integrated PR 0 tip passed:

```sh
cargo fmt --all -- --check
cargo check --locked --bench read_path_memory
cargo check --locked --no-default-features --bench read_path_memory
cargo clippy --locked --bench read_path_memory -- -D warnings
cargo test --locked
cargo test --locked --no-default-features
```

The two full test commands used the disk-backed lead target directory after
disposable `/tmp` worktree build artifacts reached their filesystem quota. The
initial quota failure occurred during linking and was not recorded as a test
failure; both complete reruns passed.

## Ratio methodology

For every future before/after claim, run both variants on the same machine, in
the same run, with the same corpus seed/configuration, feature set, build
profile, query order, warm-up policy, sample count, and concurrency. Report
paired ratios from those measurements (for example, off/on p50), preserving
absolute timings and row/violation equality beside each ratio. Do not compare a
debug result with a release result, a different host, or a different corpus as
an optimization ratio. The 2,000-graph release matrix above is still a smoke
baseline and must be superseded by the final deterministic-corpus run.
