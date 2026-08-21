# SHACL binding and settlement policy

An SHACL binding associates one data graph with one shapes graph. Graph CRDT
state is authoritative; binding status and reports are derived durable state.

## Policies

- `Enforce` evaluates the isolated local candidate before commit. An invalid
  report or validator error rejects the local checked write without changing
  graph, clock, qv, search, diagnostics, or binding state.
- `Advisory` never rejects the source transition. The binding becomes durably
  `Pending` with that transition, then settles to a complete `Valid`,
  `Invalid`, or `Failed` result when all version fences still match.
- `Disabled` skips SHACL for that binding. Existing checked RO-Crate rules
  remain active and stale verdicts are not exposed as current.

Remote CRDT records remain apply-first. A local SHACL violation or settlement
failure never vetoes or rolls back an accepted remote record.

## Locking and retry

The lock order is graph publish/write lock (sync only), target data graph
commit guard, then the short binding metadata guard. Code holding the binding
guard does not acquire a graph commit guard. Shape writers take only their own
graph guard before the metadata guard, so no path waits for a second graph
guard and the order has no lock cycle.

For a checked write Craqle:

1. acquires the target data graph commit guard;
2. briefly clones active binding records and dependency versions under the
   binding metadata guard;
3. releases the metadata guard and compiles and validates one immutable data
   snapshot;
4. reacquires the metadata guard immediately before the atomic source and
   Pending/final-state transition;
5. rechecks binding identity and policy, the data base version, root/import
   versions, schema fingerprint, and compiler model version; and
6. retries a changed snapshot, returning a precise schema-change error after
   the bounded retry limit.

The expensive compiler and validator never run while the global binding
metadata guard is held. Writes to the same data graph remain serialized by its
commit guard, while independent data graphs validate concurrently.

## Cheap status reads

Ordinary `shacl_binding_statuses` calls authorize the data graph, clone the
persisted records under the metadata guard, release it, and authorize the root
and already-recorded imported graph IDs. They compare the stored data, shape,
import, and compiler model versions with current versions.

When every fence matches, the persisted complete report is returned. A
mismatch returns `Pending` without a report. Status reads do not compile
schemas, discover imports, scan complete shapes graphs, or take an exclusive
data graph commit guard. Runtime counters report records and version checks.

## Durable Pending recovery

Pending state and one queue key per data graph commit atomically. A final
status and removal of that graph's queue key also commit atomically. Repeated
restart cannot duplicate a final report, and a graph not settled within a
startup budget remains queued.

`PendingReplayPolicy` selects all-before-open, bounded, or deferred startup
work. The default bounded policy settles at most 100 graphs or 250 ms. Public
queue status and bounded replay APIs allow worker-facing continuation without
validating during the status read.

Healthy opens scan only the durable queue. A store schema marker gates the
one-time legacy repair that scans binding records. Full binding scanning also
remains available through the explicit repair API, which reconstructs missing
queue entries and removes malformed ones.

Startup outcomes record binding records scanned, pending queue entries
scanned, graphs settled, reports produced, failures, budget exhaustion, and
elapsed time.

## Post-commit failures

Once a source transition commits, settlement errors are not returned as a
rejected write. The affected binding stays durably `Pending`, its queue key is
retained, a structured error event records graph, binding, data version, and
error, and the settlement-failure counter increments. One failed graph does
not stop other queued graphs. Explicit replay and restart retry the work, and
no failure path writes a false `Valid` state.

## Authorization and trusted writes

Binding requires write permission on the data graph and read permission on
active data and shapes/import graphs. Status reads require the same data and
recorded shape/import visibility as before this hardening. Unauthorized callers
receive an error without report leakage.

Unchecked and bulk-unchecked methods accept trusted input and bypass local
pre-commit Enforce rejection. They still preserve CRDT liveness and invalidate
affected derived state. There is no public benchmark-only deferred loader.
