<!-- Runs and interprets the RO-Crate comparison between Craqle and a local Virtuoso. -->
<!-- Copyright (c) 2026 ArunaStorage Team @ JLU Giessen -->
<!-- SPDX-License-Identifier: MIT -->

# RO-Crate benchmark: Craqle and Virtuoso

One command generates the fixture, runs both engines on the same CPUs, validates every
answer against the generator's own expected results, and writes machine-readable records:

```bash
python3 scripts/rocrate/main.py --profile smoke --out /tmp/rocrate-smoke --samples 5
```

`--profile` selects `smoke` (50 crates, about 20 entities each), `development` (1,000 crates,
about 100 entities each), `scale` (10,000 crates), or a `sweep-*` split of a fixed entity
count across different crate counts. `--tuned` applies a modest Virtuoso buffer setting
from its RDF performance guide; `--engines` runs one engine only. The run exits non-zero
when any answer is wrong, except for the documented Craqle search result limit.

## What is compared

- Fixtures are RO-Crate 1.2 metadata documents with a descriptor, a root dataset, a nested
  dataset, files, people, organizations, a license, and a `CreateAction`. Each crate is one
  named graph whose IRI is the crate root. Data entities use absolute IRIs.
- Craqle ingests each JSON-LD document through `apply_rocrate_document_with_policy`.
  Virtuoso bulk-loads the generator's canonical N-Quads. The runner checks that Craqle's
  stored quads equal those N-Quads; the only mapping is the metadata descriptor, which
  Craqle stores as the relative id `ro-crate-metadata.json`.
- Queries RC01 to RC14 use portable SPARQL with explicit `GRAPH` patterns, so both engines
  read one named graph per crate. Explicit crate sets use Craqle's graph list API and
  `FROM` clauses for Virtuoso.
- Controlled text matching compares exact resource sets for whole words in `name`,
  `description`, `keywords`, and `identifier`. Ranked text checks each engine's own top-k
  for eligibility and duplicates; the two rankings are not expected to agree.
- Readable-graph cases restrict Craqle through an authorizer and Virtuoso through a
  `VALUES ?g` list. The Virtuoso form is an application-enforced simulation, not the same
  access control.

## Boundaries

Craqle runs in-process through its public API. Virtuoso runs in a disposable local
container, pinned by image digest, bound to `127.0.0.1`, with a random password and
runner-owned data. Queries use one persistent HTTP connection because the Virtuoso ODBC
driver is not installed on the host; client decoding is part of each timing. Result caps
and query time limits are raised, and any non-200 answer or SQL state is a failure.

Craqle acknowledges each crate after a synchronous durable write; the Virtuoso bulk load
is a separate ingestion experiment. Text freshness uses Craqle's search flush and
Virtuoso's text-index update procedure.

Timing includes complete result consumption. First-process, cold-cache, and concurrent
profiles are not measured by this runner.
