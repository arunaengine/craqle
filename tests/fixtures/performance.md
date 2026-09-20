# Deterministic performance corpus

The performance corpus is generated at iteration time; large RDF data is not
checked into this repository. The checked-in golden file is only a tiny output
prefix used to guard the generator's deterministic contract.

The generator version is `performance-corpus-v1` and the default seed is
`0x435241514c455030`. Reproduce the focused checks with:

```text
cargo test --locked --test performance_corpus
```

For a configured quad count `N` and duplicate percentage `P`, exactly
`floor(N * P / 100)` output slots reuse the same subject-predicate-object
payload from a canonical slot in another graph. Non-zero duplication is
therefore rejected for the one-graph dimension.

The records are synthetic and have no external attribution. They are covered
by the repository's license.
