# Performance v1 baseline

This is the pre-PR0 smoke baseline for the `perf-v1-rocrate13-shacl`
integration branch. It is evidence for repeatable comparisons, not a release
claim. The measurements below were supplied from the same host and should be
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

| Shape | Optimizer off | Optimizer on |
| --- | ---: | ---: |
| S01 ASK | 448.981us | 410.329us |
| S02 LIMIT25 | 1.136872ms | 1.192234ms |
| S03 selective-last BGP | 14.886215ms | 564.408us |
| S04 selective-first | 489.377us | 628.408us |
| S05 OPTIONAL | 1.716838ms | 1.861569ms |
| S06 filter-eq | 21.038196ms | 601.107us |
| S07 UNION | 1.983318ms | 2.044552ms |
| S08 graph-var | 1.38685ms | 1.265132ms |
| S09 EXISTS | 35.60399ms | 2.091771ms |
| S10 NOT EXISTS | 34.00379ms | 2.081753ms |
| S11 chain | 5.904979ms | 5.191221ms |
| S12 anchored worst-order | 15.046225ms | 771.817us |
| S13 DISTINCT | 1.084463ms | 1.145778ms |
| S14 order selective | 656.601us | 727.363us |
| S15 order corpus | 78.35113ms | 79.259824ms |
| S16 contains | 20.783258ms | 20.334638ms |

The supplied release smoke also passed:

```sh
CRAQLE_MATRIX_GRAPH_COUNT=2000 CRAQLE_MATRIX_SAMPLES=5 \
CRAQLE_MATRIX_CONCURRENCY=4 \
cargo test --release --locked --test perf_query_matrix \
  query_plan_matrix_before_after -- --ignored --nocapture
```

It loaded 2,000 graphs / 1,960 live graphs in `204.107551ms`. The p50 query
timings were:

| Shape | Optimizer off | Optimizer on |
| --- | ---: | ---: |
| ASK | 25.628us | 23.424us |
| LIMIT25 | 126.536us | 129.642us |
| selective-last BGP | 4.982801ms | 34.224us |
| selective-first | 27.732us | 33.632us |
| property-ish OPTIONAL | 242.152us | 223.297us |
| filter-eq | 5.390092ms | 27.522us |
| UNION50 | 237.934us | 241.861us |
| graph-var25 | 148.156us | 139.150us |
| EXISTS | 7.852528ms | 269.213us |
| NOT EXISTS | 10.268138ms | 317.132us |
| chain100 | 884.231us | 769.997us |
| anchored worst order | 5.298771ms | 65.462us |
| DISTINCT25 | 120.835us | 126.045us |
| ordered selective | 42.249us | 48.140us |
| ordered corpus | 18.851651ms | 18.278481ms |
| contains scan | 6.547322ms | 7.125381ms |

These are debug and small release smoke runs over the existing matrix, not the
final deterministic corpus and not an SLO or release-performance claim.

## Required metric capture

No pre-instrumentation counters are inferred from elapsed time. A value is
listed only where the supplied run recorded it.

| Required metric | Baseline value | Status / capture note |
| --- | --- | --- |
| Wall time | Debug fixture load `843.406222ms`; release fixture load `204.107551ms` | Partial; query p50s are above |
| First row / violation | — | Pending instrumentation |
| Candidate quads | — | Pending instrumentation |
| Matching quads | — | Pending instrumentation |
| Seeks | — | Pending instrumentation |
| Graphs | Debug configured `1,000`; release `2,000` (`1,960` live supplied for release) | Partial; final corpus pending |
| Decodes | — | Pending instrumentation |
| Emitted rows | — | Pending instrumentation |
| Shape/focus pairs | Existing S01–S16 labels only | Pending deterministic-corpus attribution |
| Path edges | — | Pending instrumentation |
| Allocated bytes | — | Pending instrumentation |
| Peak RSS | — | Pending instrumentation |
| Write p50 / p95 / p99 | — | Pending write harness/instrumentation |
| Database bytes | — | Pending measurement harness |

## Known correctness and comparison gaps

The current union-default path has an observed duplicate-row baseline defect
when duplicate triple payloads are present across graphs. This is recorded as a
known pre-PR0 defect; the intended union-default duplicate regression remains
ignored until PR3 changes the read/index path. PR0 does not alter production
query semantics.

An external Rudof SHACL comparison cannot run against this dependency state.
It remains pending until the verified opt-in Rudof/SHACL dependencies land in
PR1; no external validation result is fabricated here.

## Ratio methodology

For every future before/after claim, run both variants on the same machine, in
the same run, with the same corpus seed/configuration, feature set, build
profile, query order, warm-up policy, sample count, and concurrency. Report
paired ratios from those measurements (for example, off/on p50), preserving
absolute timings and row/violation equality beside each ratio. Do not compare a
debug result with a release result, a different host, or a different corpus as
an optimization ratio. The 2,000-graph release matrix above is still a smoke
baseline and must be superseded by the final deterministic-corpus run.
