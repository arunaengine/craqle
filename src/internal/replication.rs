use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use crate::core::*;
use crate::rules::{ChangeSet, DeltaSummary, Rule};
use crate::sparql::SparqlEngine;
use crate::store::{
    BatchTermCtx, ClockUpdate, CounterKey, EncodedQuad, FtsEnqueue, FtsSubject, GraphStore,
    QuadAdd, QuadRemove, TermId,
};
use chrono::Utc;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("sparql: {0}")]
    Sparql(#[from] crate::sparql::SparqlError),
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<CrateViolation>),
    #[error("invalid change set: {0}")]
    InvalidChangeSet(String),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("sync: {0}")]
    Sync(#[from] crate::sync::CraqleSyncError),
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("input rejected: {0}")]
    InputRejected(String),
}

#[derive(Debug)]
pub struct MergeResult {
    pub applied: bool,
}

/// Number of shards backing [`GRAPH_WRITE_LOCKS`].
const GRAPH_WRITE_LOCK_SHARDS: usize = 32;

/// Orders one graph's *engine-level* write pipeline — the part that happens
/// before and around the store's own commit guard. Two things need it:
///
/// **1. The sync publish must be applied in publish order (G1, G2, G3).**
/// `IrokleGraphSync::publish_changes` stamps the batch with the publishing
/// actor's next irokle sequence number, and the local apply gates on
/// `VectorClock::contains`, which is monotonic per actor. Two threads publishing
/// to the same graph get sequence numbers `n` and `n+1`, but nothing stops the
/// thread holding `n+1` from reaching the apply first: the clock then advances
/// to `n+1` and the `n` batch is discarded as "already applied", silently losing
/// every add in it. Holding this lock across publish *and* apply makes publish
/// order the apply order.
///
/// **2. The `@context` tag mint must be atomic (G5).** Without it two concurrent
/// local context writes both read the same stored tag and
/// `ContextTag::next_local` mints the *identical* `(counter, actor)` for two
/// different context values — a tie the last-write-wins register cannot resolve,
/// so peers need not converge.
///
/// This deliberately is **not** `GraphStore::graph_commit_guard`. Both uses have
/// to span a call that takes that guard internally — `GraphStore::set_graph_context`
/// and, on a first publish, `ensure_graph_topic` → `set_irokle_topic_id` — and
/// `std::sync::Mutex` is not reentrant, so reusing the store guard would
/// self-deadlock (addendum A1). Releasing it in between would reopen both races.
///
/// Process-wide (a `static`) rather than per-engine: one store is shared by
/// several `ReplicationEngine`s — the sync engine, the local-only engine, and the
/// per-request engines built for explicit actors — so an engine-local mutex would
/// not serialize them. Sharded by graph IRI; a collision only serializes two
/// unrelated graphs.
///
/// Lock order: **graph write lock ▸ graph commit guard**. Nothing acquires them
/// the other way round, and neither is ever taken twice on one path.
static GRAPH_WRITE_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| {
    (0..GRAPH_WRITE_LOCK_SHARDS)
        .map(|_| Mutex::new(()))
        .collect()
});

fn graph_write_lock(graph: &GraphId) -> &'static Mutex<()> {
    let hash = blake3::hash(graph.as_str().as_bytes());
    let shard = u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()) as usize;
    &GRAPH_WRITE_LOCKS[shard % GRAPH_WRITE_LOCK_SHARDS]
}

/// Acquire a graph's engine-level write lock; see [`GRAPH_WRITE_LOCKS`] for
/// what it orders and for the lock order it belongs to.
pub(crate) fn graph_write_guard(graph: &GraphId) -> MutexGuard<'static, ()> {
    graph_write_lock(graph)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The replication engine: local writes and CRDT merge of Irokle records.
pub struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    rules: Vec<Box<dyn Rule>>,
    actor: ActorId,
    sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
}

/// How a write should leave the graph's persisted diagnostics record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsPlan {
    /// Refresh the record as part of this commit, under its guard.
    Immediate,
    /// Bulk import: the caller rebuilds diagnostics once at the end.
    Deferred,
}

impl DiagnosticsPlan {
    /// Run `capture` only when this write refreshes diagnostics immediately.
    fn pending_diagnostics<F>(&self, capture: F) -> crate::store::Result<Option<PendingDiagnostics>>
    where
        F: FnOnce() -> crate::store::Result<PendingDiagnostics>,
    {
        match self {
            Self::Immediate => capture().map(Some),
            Self::Deferred => Ok(None),
        }
    }
}

/// A local write, ready to be committed to one graph.
struct LocalCommit<'a> {
    graph: &'a GraphId,
    changes: Vec<MaterializedQuadChange>,
    plan: DiagnosticsPlan,
}

impl ReplicationEngine {
    pub fn new(store: Arc<GraphStore>, sparql: Arc<SparqlEngine>, actor: ActorId) -> Self {
        Self::new_with_sync(store, sparql, actor, None)
    }

    pub fn new_with_sync(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    ) -> Self {
        Self {
            store,
            sparql,
            rules: crate::rules::default_rules(),
            actor,
            sync,
        }
    }

    pub fn store(&self) -> &Arc<GraphStore> {
        &self.store
    }

    /// Persist a graph's raw RO-Crate render hints (last-write-wins) and, when
    /// sync is configured, replicate the change to peers so their exports match.
    ///
    /// A fresh ordering tag is minted here (`stored_counter + 1`, actor =
    /// this engine's actor) and used for both the local store and the published
    /// event, so peers apply the same deterministic last-write-wins resolution.
    ///
    /// Publish-first invariant (load-bearing). The `ContextUpdated` event is
    /// published to peers *before* the local store is updated. This ordering
    /// makes the operation self-healing: if the publish fails, the local stored
    /// hints are left unchanged and a retry re-mints the same-or-higher tag and
    /// re-publishes. Reversing the order (store locally, then publish) would, on
    /// a publish failure, leave the local hints updated so that a retry trips
    /// the unchanged-state short-circuit in `store_import_context` and never
    /// re-publishes — leaving peers permanently without the update.
    pub fn set_graph_context(
        &self,
        graph: &GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
    ) -> Result<(), UpdateError> {
        // Bind the topic before taking any lock. `ensure_graph_topic` reaches
        // `GraphStore::set_irokle_topic_id`, which takes the graph commit guard
        // itself; doing it under a lock we also hold would deadlock (addendum
        // A1). It is idempotent and, after the first call, a memo hit.
        if let Some(sync) = &self.sync {
            sync.ensure_graph_topic(&self.store, graph)?;
        }

        // Guards the context-tag mint through to the store write; see
        // GRAPH_WRITE_LOCKS.
        let _write_guard = graph_write_guard(graph);

        let tag = ContextTag::next_local(self.store.graph_context_tag(graph)?, self.actor);
        if let Some(sync) = &self.sync {
            sync.publish_context(
                &self.store,
                graph,
                context.clone(),
                license.clone(),
                license_digest,
                tag,
            )?;
        }
        self.store.set_graph_context(
            graph,
            context.as_deref(),
            license.as_deref(),
            license_digest,
            tag,
        )?;
        Ok(())
    }

    /// Execute a SPARQL Update locally with full validation.
    /// Returns `None` if the update produced no changes.
    pub fn local_update(&self, sparql_update: &str) -> Result<Option<Batch>, UpdateError> {
        let changes = self.sparql.evaluate_update(sparql_update)?;

        if changes.is_empty() {
            return Ok(None);
        }

        let graph = match &changes[0] {
            MaterializedQuadChange::Insert { graph, .. }
            | MaterializedQuadChange::Delete { graph, .. } => graph.clone(),
        };
        self.ensure_change_set_targets(&graph, &changes)?;
        self.validate(&graph, &changes)?;

        self.commit_changes(&graph, changes).map(Some)
    }

    /// Insert raw quads (bypasses SPARQL, still validates).
    pub fn local_insert_quads(
        &self,
        graph: &GraphId,
        quads: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
    ) -> Result<Batch, UpdateError> {
        let changes: Vec<MaterializedQuadChange> = quads
            .into_iter()
            .map(|(s, p, o)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: s,
                predicate: p,
                object: o,
            })
            .collect();

        self.local_apply_changes(graph, changes)
    }

    /// Apply a pre-materialized change set locally with full validation.
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str(), change_count = changes.len()))]
    pub fn local_apply_changes(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.validate(graph, &changes)?;
        self.commit_changes(graph, changes)
    }

    /// Apply a pre-materialized change set locally without full graph validation.
    ///
    /// Intended for trusted higher-level RO-Crate operations that maintain
    /// structural invariants incrementally.
    pub fn local_apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Immediate,
        })
    }

    /// Apply a trusted bulk change set locally and defer graph-diagnostics
    /// recomputation until the caller explicitly rebuilds diagnostics.
    pub fn local_apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Deferred,
        })
    }

    pub fn rebuild_graph_diagnostics(&self, graph: &GraphId) -> Result<(), UpdateError> {
        // Guards the recompute→persist cycle so the record cannot be tagged with
        // a clock newer than the state it describes.
        let _commit_guard = self.store.graph_commit_guard(graph);
        // The last persisted set is what the search index reflects, so it is the
        // right thing to diff against. Reading it through `graph_diagnostics`
        // would recompute a stale record first and lose the difference.
        let previous = self
            .store
            .last_persisted_diagnostics(graph)
            .map_err(UpdateError::Store)?;
        self.recompute_graph_diagnostics(graph, &previous)
            .map_err(UpdateError::Store)
    }

    fn empty_batch(&self, graph: &GraphId) -> Result<Batch, UpdateError> {
        Ok(Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter: 0,
            base_clock: self.store.get_vector_clock(graph)?,
            ops: vec![],
            timestamp: Utc::now(),
        })
    }

    fn validate(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        let change_set = ChangeSet {
            store: &self.store,
            graph,
            delta: changes,
        };
        match crate::rules::validate_change_set(&self.rules, change_set) {
            Ok(()) => Ok(()),
            Err(crate::rules::RuleEvaluationError::Store(error)) => Err(UpdateError::Store(error)),
            Err(crate::rules::RuleEvaluationError::Violations(violations)) => {
                Err(UpdateError::ValidationFailed(violations))
            }
        }
    }

    fn ensure_change_set_targets(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        for change in changes {
            let change_graph = match change {
                MaterializedQuadChange::Insert { graph, .. }
                | MaterializedQuadChange::Delete { graph, .. } => graph,
            };
            if change_graph != graph {
                return Err(UpdateError::InvalidChangeSet(format!(
                    "all changes must target `{}` but found `{}`",
                    graph.as_str(),
                    change_graph.as_str()
                )));
            }
        }
        Ok(())
    }

    /// Internal: assign dots, write to store, build replication batch.
    fn commit_changes(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Immediate,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %commit.graph.as_str(), change_count = commit.changes.len(), sync_enabled = self.sync.is_some()))]
    fn commit_changes_with_plan(&self, commit: LocalCommit<'_>) -> Result<Batch, UpdateError> {
        let LocalCommit {
            graph,
            changes,
            plan,
        } = commit;

        if let Some(sync) = &self.sync {
            // Orders this graph's publish against its own apply; see
            // GRAPH_WRITE_LOCKS. Taken before the publish and held across it.
            let _write_guard = graph_write_guard(graph);

            // Publish-first (G4): the event goes out before any local state
            // changes, and outside the commit guard, because the publish may bind
            // an irokle topic and that takes the guard itself (addendum A1).
            let record = sync.publish_changes(&self.store, graph, changes)?;
            let Some(batch) = crate::sync::batch_from_irokle_event_record(record)? else {
                return Err(UpdateError::InvalidChangeSet(
                    "irokle changes publish did not return a quad-change record".to_string(),
                ));
            };
            self.apply_irokle_batch_with_plan(&batch, plan)
                .map_err(update_error_from_merge)?;
            return Ok(batch);
        }

        // `create_graph` is self-guarding, so the graph must exist *before* the
        // commit guard is taken; the guard is not reentrant. (The sync branch
        // leaves this to the apply, which does the same thing before its guard.)
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Guards the whole read→write cycle of this graph's CRDT state: the
        // diagnostics read, the clock read, the counter mint, every quad op, the
        // clock write, the FTS enqueue, the commit and the diagnostics refresh
        // (G1, G2, G5, G6).
        let _commit_guard = self.store.graph_commit_guard(graph);

        // Captured before the write, under the guard, so it describes exactly
        // the state this commit starts from. Skipped entirely when the caller
        // defers the refresh — reading it can itself force a recompute.
        let pending = plan.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_delta(graph, &changes),
            })
        })?;

        let mut batch = self.store.new_batch();
        let mut vector_clock = self.store.get_vector_clock(graph)?;
        let graph_id = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;
        let counter = self.store.next_counter(
            &mut batch,
            CounterKey {
                graph_id,
                actor: self.actor,
            },
        )?;
        let dot = Dot {
            actor: self.actor,
            counter,
        };
        let base_clock = vector_clock.clone();

        let mut ops = Vec::with_capacity(changes.len());
        let mut affected_subjects = HashSet::new();
        let mut term_cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut term_cache,
        };

        self.store.seed_term_cache(
            &mut cx,
            changes.iter().flat_map(|change| match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                }
                | MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => [subject, predicate, object],
            }),
        )?;

        for change in changes {
            match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject: &subject,
                            predicate: &predicate,
                            object: &object,
                        },
                    )?;
                    self.store.insert_quad(cx.batch, QuadAdd { quad, dot })?;
                    affected_subjects.insert(quad.subject);
                    ops.push(QuadOp::Add {
                        subject,
                        predicate,
                        object,
                        dot,
                    });
                }
                MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject: &subject,
                            predicate: &predicate,
                            object: &object,
                        },
                    )?;
                    self.store.remove_quad(
                        cx.batch,
                        QuadRemove {
                            quad,
                            witnessed: &vector_clock,
                        },
                    )?;
                    affected_subjects.insert(quad.subject);
                    ops.push(QuadOp::Remove {
                        subject,
                        predicate,
                        object,
                        witnessed: vector_clock.clone(),
                    });
                }
            }
        }

        vector_clock.advance(self.actor, counter);
        self.store.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: &vector_clock,
            },
        )?;
        self.store.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &affected_subjects,
            },
        )?;
        self.store.commit(batch)?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)
                .map_err(UpdateError::Store)?;
        }

        Ok(Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter,
            base_clock,
            ops,
            timestamp: Utc::now(),
        })
    }

    fn resolve_quad(
        &self,
        cx: &mut BatchTermCtx<'_>,
        terms: QuadTerms<'_>,
    ) -> crate::store::Result<EncodedQuad> {
        Ok(EncodedQuad {
            graph: terms.graph_id,
            subject: self.store.resolve_term_cached(cx, terms.subject)?,
            predicate: self.store.resolve_term_cached(cx, terms.predicate)?,
            object: self.store.resolve_term_cached(cx, terms.object)?,
        })
    }

    /// Apply a causally ordered batch produced from an Irokle graph event.
    ///
    /// Irokle actor sequences include genesis and topic-control operations, so
    /// they are not contiguous over Craqle domain events. The Irokle DAG already
    /// enforces causal delivery; this path intentionally bypasses Craqle's old
    /// vector-clock gap buffering while preserving OR-Set add/remove semantics.
    pub fn apply_irokle_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        self.apply_irokle_batch_with_plan(&incoming, DiagnosticsPlan::Immediate)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %incoming.graph.as_str(), op_count = incoming.ops.len()))]
    fn apply_irokle_batch_with_plan(
        &self,
        incoming: &Batch,
        plan: DiagnosticsPlan,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        // Self-guarding, so it must run before the commit guard is taken.
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Guards the dedup gate through to the diagnostics refresh: the clock
        // read that decides "already applied" must be the same clock the apply
        // then advances, or a concurrent commit can make one batch apply twice
        // or the clock lose an entry (G1, G2).
        let _commit_guard = self.store.graph_commit_guard(graph);

        let mut vector_clock = self.store.get_vector_clock(graph)?;
        if vector_clock.contains(&Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        }) {
            return Ok(MergeResult { applied: false });
        }

        let pending = plan.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_ops(graph, &incoming.ops),
            })
        })?;

        self.apply_single_batch(incoming, &mut vector_clock)?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)?;
        }
        Ok(MergeResult { applied: true })
    }

    pub fn apply_irokle_record(
        &self,
        record: &irokle::reducer::EventRecord<crate::sync::CraqleGraphEvent>,
    ) -> Result<Option<MergeResult>, MergeError> {
        let batch = crate::sync::batch_from_irokle_record(record)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        batch
            .map(|batch| self.apply_irokle_batch(batch))
            .transpose()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %incoming.graph.as_str(), op_count = incoming.ops.len()))]
    fn apply_single_batch(
        &self,
        incoming: &Batch,
        vector_clock: &mut VectorClock,
    ) -> Result<(), MergeError> {
        let graph = &incoming.graph;
        let mut batch = self.store.new_batch();
        let mut affected_subjects = HashSet::new();
        let mut term_cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut term_cache,
        };

        self.store.seed_term_cache(
            &mut cx,
            incoming.ops.iter().flat_map(|op| match op {
                QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    ..
                }
                | QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    ..
                } => [subject, predicate, object],
            }),
        )?;

        let graph_id = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;

        for op in &incoming.ops {
            match op {
                QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject,
                            predicate,
                            object,
                        },
                    )?;
                    self.store
                        .insert_quad(cx.batch, QuadAdd { quad, dot: *dot })?;
                    affected_subjects.insert(quad.subject);
                }
                QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject,
                            predicate,
                            object,
                        },
                    )?;
                    self.store
                        .remove_quad(cx.batch, QuadRemove { quad, witnessed })?;
                    affected_subjects.insert(quad.subject);
                }
            }
        }

        vector_clock.advance(incoming.actor, incoming.counter);
        self.store.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: vector_clock,
            },
        )?;
        self.store.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &affected_subjects,
            },
        )?;
        self.store.commit(batch)?;
        Ok(())
    }

    /// Bring the persisted diagnostics record back in step with the state the
    /// commit just produced. **Call with the graph commit guard held.**
    ///
    /// Two cases, cheapest first:
    ///
    /// 1. The write cannot have moved the orphan set, so the previous verdict is
    ///    re-stamped against the new clock.
    /// 2. Anything else: recompute from the store.
    ///
    /// Passing validation is deliberately *not* a third case. `validate` runs
    /// before the commit guard is taken, so two writes that each validate
    /// against the same pre-state can both pass and still orphan an entity
    /// between them (two deletes, each cutting one of an entity's two parents).
    /// Asserting "validated, therefore orphan-free" would then persist that lie
    /// under a matching clock tag, where no reader would ever correct it. The
    /// orphan set is derived state, so it is derived (G6).
    fn settle_diagnostics(
        &self,
        graph: &GraphId,
        pending: &PendingDiagnostics,
    ) -> crate::store::Result<()> {
        // Case 1. `orphaned_data_entities` reads exactly two triple shapes:
        // `?s rdf:type schema:Dataset|schema:MediaObject` (which entities count
        // as data entities) and `?s schema:hasPart ?o` (which adds to that set
        // and forms every edge of the reachability graph). Nothing else in the
        // graph can affect it. `touches_reachability` is set by exactly those
        // two shapes, so a write that leaves it clear provably leaves the orphan
        // set identical — the previous verdict is still exact and only needs
        // re-stamping so its clock tag matches the new state (G6).
        if !pending.summary.touches_reachability() {
            return self.store.set_graph_diagnostics(graph, &pending.previous);
        }

        // Case 2. `pending.previous` was captured before the write; re-reading
        // it here would yield the post-write set and defeat the search re-queue.
        self.recompute_graph_diagnostics(graph, &pending.previous)
    }

    /// Recompute the orphan set from the store and persist it, re-queueing the
    /// entities whose visibility changed for search (G6, G7).
    ///
    /// `previous` must be the orphan set as it stood *before* the write. It
    /// cannot be re-read here: the commit has already advanced the graph clock,
    /// so a read now finds the stored record's tag stale and recomputes it from
    /// post-write state — which would make `previous` equal `current` every
    /// time, and silently skip the search re-queue for any entity whose
    /// visibility the write flipped without touching it directly.
    ///
    /// The re-queue comes first and the record second, in that order: the two
    /// are separate commits, and a crash between them must leave the older
    /// baseline behind so the next rebuild re-queues, never the newer one with
    /// nothing enqueued (G7).
    fn recompute_graph_diagnostics(
        &self,
        graph: &GraphId,
        previous: &GraphDiagnostics,
    ) -> crate::store::Result<()> {
        // Recomputes against post-write state, because the commit already made
        // the stored record's clock tag stale. It does not persist: this is the
        // writer that owns the record.
        let current = self.store.graph_diagnostics(graph)?;
        if *previous != current {
            self.enqueue_orphan_fts_updates(
                graph,
                OrphanChange {
                    previous: previous.clone(),
                    current: current.clone(),
                },
            )?;
        }
        self.store.set_graph_diagnostics(graph, &current)
    }

    fn enqueue_orphan_fts_updates(
        &self,
        graph: &GraphId,
        change: OrphanChange,
    ) -> crate::store::Result<()> {
        let previous: HashSet<&String> = change.previous.orphaned_entities.iter().collect();
        let current: HashSet<&String> = change.current.orphaned_entities.iter().collect();
        let Some(graph_id) = self
            .store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))?
        else {
            return Ok(());
        };
        let mut batch = self.store.new_batch();
        let mut dirty = false;

        for entity_id in previous.symmetric_difference(&current) {
            // `from_subject_id`, not `from_named_node`: diagnostics store a
            // blank node as `_:b0`, and re-encoding that as the IRI `<_:b0>`
            // would miss the lookup and silently never re-index it (G6, G7).
            let subject = EncodedTerm::from_subject_id(entity_id.as_str());
            let Some(subject_tid) = self.store.lookup_term(&subject)? else {
                continue;
            };
            self.store.enqueue_fts(
                &mut batch,
                FtsSubject {
                    graph_id,
                    subject: subject_tid,
                },
            )?;
            dirty = true;
        }

        if dirty {
            self.store.commit(batch)?;
        }

        Ok(())
    }
}

/// The four term ids of one quad, before interning.
struct QuadTerms<'a> {
    graph_id: TermId,
    subject: &'a EncodedTerm,
    predicate: &'a EncodedTerm,
    object: &'a EncodedTerm,
}

/// Diagnostics inputs captured before a write, under the commit guard.
struct PendingDiagnostics {
    /// The record as it stood before the commit.
    previous: GraphDiagnostics,
    /// What the write touches, in the terms the orphan set depends on.
    summary: DeltaSummary,
}

/// The orphan set before and after a recompute.
struct OrphanChange {
    previous: GraphDiagnostics,
    current: GraphDiagnostics,
}

fn update_error_from_merge(error: MergeError) -> UpdateError {
    match error {
        MergeError::Store(error) => UpdateError::Store(error),
        MergeError::InputRejected(message) => UpdateError::InvalidChangeSet(message),
    }
}
