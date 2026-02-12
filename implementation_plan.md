# Implementation Plan: RO-Crate CRDT Mockup

## Purpose

This document describes how to build a working Rust prototype that validates the core thesis: RO-Crate metadata can be stored as RDF named graphs in a Fjall-backed Oxigraph fork, replicated between peers using an OR-Set CRDT over concrete quad operations with dotted causality, protected by SHACL-compiled pre-execution guards, searched through a Tantivy sidecar index, and synchronized without any network layer by using Rust channels as the transport.

The prototype is not a production system. It exists to prove that the merge semantics, the SHACL enforcement, the post-merge reachability checks, and the search reindexing pipeline all work together correctly under concurrent offline edits. Every architectural shortcut in the prototype should be documented so that the path from mockup to production is clear.

---

## 1. Crate Layout

The workspace should contain nine crates. Each crate has a single responsibility, and the dependency graph is strictly layered: lower crates never depend on higher ones.

### 1.1 `aruna-core`

Foundational types shared across the entire workspace.

**Types to define:**

- `GraphId`: a newtype over `oxrdf::NamedNode` representing one RO-Crate's named graph IRI.
- `ActorId`: a newtype over `uuid::Uuid`. Each simulated peer gets one at startup. The actor ID is stable for the lifetime of that peer instance.
- `Dot`: a struct with fields `actor: ActorId` and `counter: u64`. This is the event identifier for a single committed batch on a single actor. The counter is monotonically increasing per actor per graph.
- `Frontier`: a newtype over `BTreeMap<ActorId, u64>`. This records the highest counter seen from each actor for a given graph. The frontier enables catch-up: a receiver tells the sender "I have seen everything up to frontier F," and the sender streams batches whose dots are beyond F.
- `QuadOp`: an enum with two variants:
  - `Add { subject: EncodedTerm, predicate: EncodedTerm, object: EncodedTerm, dot: Dot }`. The graph component is implicit from the batch context.
  - `Remove { subject: EncodedTerm, predicate: EncodedTerm, object: EncodedTerm, witnessed: Frontier }`. The `witnessed` frontier records the causal context the removing actor had observed at the time of the remove. This is required for observed-remove semantics: a concurrent add from an actor not yet in the witnessed frontier survives the remove.
- `Batch`: the unit of replication. Fields:
  - `graph: GraphId`
  - `actor: ActorId`
  - `counter: u64`
  - `base_frontier: Frontier` (the actor's frontier for this graph before applying this batch)
  - `ops: Vec<QuadOp>`
  - `timestamp: chrono::DateTime<Utc>` (wall-clock, informational only, not used for ordering)
- `CrateViolation`: an enum describing the kinds of structural violations the system can detect. Variants include `MissingRootDataEntity`, `MissingMetadataDescriptor`, `MissingRequiredProperty { entity: String, property: String }`, `OrphanedDataEntity { entity_id: String }`, `InvalidDatePublishedCardinality`, and `EntityMissingType { entity_id: String }`.
- `PredicateFilter`: a struct describing which predicates trigger Tantivy reindexing. Contains a `HashSet<NamedNode>` of predicates like `schema:name`, `schema:description`, `schema:keywords`.

**Dependencies:** `oxrdf`, `uuid`, `chrono`, `serde` (with derive).

All types must implement `Serialize` and `Deserialize` via serde so that batches can be serialized for the channel transport and for the Fjall batch log.

### 1.2 `aruna-rdf-store`

The Fjall-backed RDF dataset store, replacing Oxigraph's RocksDB `Storage` layer.

This is the most complex crate and the one that determines whether the overall approach is viable. It must expose enough surface area for SPARQL query evaluation (via `spareval`) while keeping the storage layout simple enough to build and test in the prototype phase.

**Fjall keyspace layout:**

The prototype should use these keyspaces. Each keyspace is a logical partition within a single Fjall instance, and cross-keyspace writes within a single `Batch` (Fjall's write batch, not to be confused with the replication `Batch` type) are atomic.

| Keyspace | Key format | Value format | Purpose |
|---|---|---|---|
| `term2id` | Serialized RDF term (via `oxrdf` encoding) | `u64` (term ID) | Map lexical terms to compact integer IDs |
| `id2term` | `u64` (term ID) | Serialized RDF term | Reverse mapping for result construction |
| `gspo` | `graph_id ++ subject_id ++ predicate_id ++ object_id` | empty | Graph-first triple index, primary for within-crate queries |
| `gpos` | `graph_id ++ predicate_id ++ object_id ++ subject_id` | empty | Predicate-object lookup within a graph |
| `gosp` | `graph_id ++ object_id ++ subject_id ++ predicate_id` | empty | Object lookup within a graph |
| `spog` | `subject_id ++ predicate_id ++ object_id ++ graph_id` | empty | Cross-graph queries without graph filter |
| `graphs` | `graph_id` | empty | Set of all named graphs |
| `graph_frontier` | `graph_id` | `Frontier` (serialized) | Per-graph vector clock |
| `actor_counter` | `graph_id ++ actor_id` | `u64` | Next counter value per actor per graph |
| `quad_dots` | `graph_id ++ subject_id ++ predicate_id ++ object_id` | `Vec<Dot>` (serialized) | Set of dots that produced each live quad. A quad is live if its dot set is non-empty. Required for OR-Set remove semantics: a remove must record the dots it witnessed, and only those dots are removed from the set. A concurrent add creates a new dot not in the witnessed set, so the quad survives. |
| `batch_log` | `graph_id ++ actor_id ++ counter` | `Batch` (serialized) | Outbound replication log. Batches are appended here on local commit and read by the sync layer for catch-up. |
| `fts_queue` | Auto-incrementing `u64` | `(GraphId, EncodedTerm)` | Queue of resources needing Tantivy reindexing |

**Key encoding:** All multi-part keys use fixed-width `u64` components encoded as big-endian bytes so that Fjall's lexicographic ordering corresponds to numeric ordering. Term IDs are `u64`. Graph IDs are mapped through the term dictionary just like any other named node.

**Term dictionary implementation:**

The term dictionary assigns a unique `u64` to each distinct RDF term. The prototype should use a simple atomic counter for ID generation, persisted in a dedicated `next_term_id` key in the `term2id` keyspace.

For encoding, the prototype should use `oxrdf`'s `NamedNode`, `BlankNode`, and `Literal` types and serialize them with `postcard` (a compact, no-std-compatible binary format that is already in your dependency tree from Fjall). The term dictionary does not need to be particularly clever for the prototype; correctness matters more than compactness.

**Core API surface:**

```rust
pub struct GraphStore {
    keyspace: fjall::Keyspace,
    // handles to each partition
}

impl GraphStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;

    // Term dictionary
    pub fn encode_term(&self, term: &impl Into<EncodedTerm>) -> Result<TermId>;
    pub fn decode_term(&self, id: TermId) -> Result<EncodedTerm>;

    // Graph lifecycle
    pub fn create_graph(&self, graph: &GraphId) -> Result<()>;
    pub fn drop_graph(&self, graph: &GraphId) -> Result<()>;
    pub fn contains_graph(&self, graph: &GraphId) -> Result<bool>;
    pub fn graphs(&self) -> Result<Vec<GraphId>>;

    // Quad operations (low level, used by SPARQL layer and CRDT layer)
    pub fn insert_quad(
        &self,
        batch: &mut fjall::WriteBatch,
        graph: TermId, subject: TermId,
        predicate: TermId, object: TermId,
        dot: &Dot,
    ) -> Result<()>;

    pub fn remove_quad(
        &self,
        batch: &mut fjall::WriteBatch,
        graph: TermId, subject: TermId,
        predicate: TermId, object: TermId,
        witnessed: &Frontier,
    ) -> Result<bool>; // returns true if quad was actually removed (all dots witnessed)

    // Quad queries (used by spareval)
    pub fn quads_for_pattern(
        &self,
        graph: Option<TermId>,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Result<impl Iterator<Item = EncodedQuad>>;

    // Frontier operations
    pub fn get_frontier(&self, graph: &GraphId) -> Result<Frontier>;
    pub fn set_frontier(
        &self,
        batch: &mut fjall::WriteBatch,
        graph: &GraphId,
        frontier: &Frontier,
    ) -> Result<()>;

    // Actor counter
    pub fn next_counter(
        &self,
        batch: &mut fjall::WriteBatch,
        graph: &GraphId,
        actor: &ActorId,
    ) -> Result<u64>;

    // Batch log
    pub fn append_batch_log(
        &self,
        write_batch: &mut fjall::WriteBatch,
        batch: &Batch,
    ) -> Result<()>;
    pub fn batches_beyond_frontier(
        &self,
        graph: &GraphId,
        frontier: &Frontier,
    ) -> Result<Vec<Batch>>;

    // FTS queue
    pub fn enqueue_fts(
        &self,
        batch: &mut fjall::WriteBatch,
        graph: &GraphId,
        subject: TermId,
    ) -> Result<()>;
    pub fn drain_fts_queue(&self, limit: usize) -> Result<Vec<(GraphId, TermId)>>;

    // Atomic commit
    pub fn commit(&self, batch: fjall::WriteBatch) -> Result<()>;
}
```

**Relationship to Oxigraph internals:**

The prototype does not fork the Oxigraph repository. Instead, it depends on `oxrdf` for term types and `spargebra` for SPARQL parsing. The `spareval` crate is where the integration gets interesting: `spareval` expects to call into a store that can resolve triple patterns, and its `PreparedDeleteInsertUpdate` type yields concrete quad mutations. The `GraphStore` must implement a trait (or adapter) that `spareval`'s evaluator can call.

In practice, this means implementing a thin adapter that maps `spareval`'s `QueryableDataset` expectations onto `GraphStore::quads_for_pattern`. The exact trait to implement depends on the `spareval` version; the prototype should pin a specific version and document the adapter surface.

If adapting `spareval` proves too costly for the prototype timeline, an alternative is to implement SPARQL Update evaluation manually for the subset of operations the prototype needs: `INSERT DATA`, `DELETE DATA`, and `DELETE/INSERT WHERE` with simple graph patterns. This is less general but gets the prototype running faster. The document should record this decision when it is made.

**Dependencies:** `fjall`, `oxrdf`, `postcard`, `serde`, `aruna-core`.


### 1.3 `aruna-sparql`

SPARQL query and update interface on top of `aruna-rdf-store`.

**Responsibilities:**

- Parse SPARQL queries and updates using `spargebra`.
- Evaluate SPARQL queries against `GraphStore` using `spareval` (or a simplified evaluator for the prototype).
- For SPARQL Update, materialize concrete quad additions and removals from the parsed algebra.
- Return the materialized quad delta as a `Vec<QuadOp>` without committing. The caller (the write pipeline in `aruna-repl`) is responsible for assigning dots, applying SHACL guards, and committing atomically.

**Key design decision:**

SPARQL Update evaluation must be snapshot-consistent. The prototype should read from a consistent snapshot of the Fjall store at the start of update evaluation and produce quad deltas against that snapshot. Fjall supports snapshot reads via its `Snapshot` type. The evaluator takes a snapshot, resolves all WHERE patterns against it, computes the concrete inserts and deletes, and returns them as a `Vec<(QuadOp, EncodedQuad)>` without writing anything.

**API surface:**

```rust
pub struct SparqlEngine {
    store: Arc<GraphStore>,
}

impl SparqlEngine {
    pub fn query(&self, sparql: &str) -> Result<QueryResults>;

    /// Evaluate a SPARQL UPDATE and return the materialized quad delta
    /// without committing. The caller assigns dots and commits.
    pub fn evaluate_update(
        &self,
        sparql: &str,
    ) -> Result<Vec<MaterializedQuadChange>>;
}

pub enum MaterializedQuadChange {
    Insert {
        graph: GraphId,
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
    },
    Delete {
        graph: GraphId,
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
    },
}
```

**Dependencies:** `spargebra`, `spareval` (or manual evaluator), `oxrdf`, `aruna-core`, `aruna-rdf-store`.


### 1.4 `aruna-shacl`

SHACL shape compilation and pre-execution validation.

This crate does not implement a general-purpose SHACL engine. It implements a compiler that reads a SHACL shapes graph (stored as RDF) and produces a set of `CompiledGuard` closures. Each guard takes a proposed quad delta and the current graph snapshot and returns either `Ok(())` or `Err(CrateViolation)`.

**What the guards enforce (base RO-Crate spec, MUST-level only):**

1. **Root Data Entity existence.** The root entity (conventionally `<./>` or the graph's own IRI) must exist with `@type` including `schema:Dataset`. The guard rejects any delete that would remove the last `rdf:type` triple from the root entity, or remove the triple `<./> rdf:type schema:Dataset`.

2. **Metadata Descriptor existence.** The entity `<ro-crate-metadata.json>` must exist with `@type CreativeWork` and must have an `about` property pointing to the root data entity. The guard rejects deletes that would break this.

3. **Required root properties.** `name`, `description`, `datePublished`, and `license` must remain present on the root data entity. The guard rejects a delete of any of these predicates from the root entity if no concurrent insert replaces it (which in a local commit context means: after applying the full delta, the root entity must still have at least one value for each of these predicates).

4. **`datePublished` single-value constraint.** After applying the delta, the root entity must have exactly one `datePublished` value. The guard rejects an insert that would create a second value.

5. **`@id` and `@type` presence per entity.** Every entity in the graph must have at least one `rdf:type` triple. The guard rejects a delete that removes the last `rdf:type` from any entity, or an insert of a new entity (a new subject IRI appearing in a quad) without a corresponding `rdf:type` triple in the same delta.

6. **`hasPart` reachability for data entities.** All data entities (entities typed `File` or `Dataset` with a relative or absolute URI as `@id`) must be reachable from the root data entity through a chain of `hasPart` properties. This guard runs after applying the full delta and performs a graph walk from the root entity's `hasPart` triples. Any data entity not reachable is a violation.

**Guard application model:**

Guards run after the SPARQL update evaluator has produced the materialized quad delta but before the delta is committed. The pipeline is:

1. Produce `Vec<MaterializedQuadChange>` from SPARQL evaluation.
2. Apply the delta to a temporary in-memory view of the affected graph (the current snapshot plus the proposed changes).
3. Run all compiled guards against this view.
4. If any guard returns a violation, reject the entire update and return the violation to the caller.
5. If all guards pass, proceed to dot assignment and commit.

**SHACL compilation:**

The prototype should ship with a hardcoded shapes graph for the base RO-Crate 1.2 spec. The shapes graph is a small Turtle file embedded as a const string. At startup, the compiler reads the shapes graph, matches each `NodeShape` to a guard implementation, and produces a `Vec<Box<dyn Guard>>`.

The compiler only needs to handle a small subset of SHACL:
- `sh:targetNode` (for the root entity and metadata descriptor)
- `sh:property` with `sh:path`, `sh:minCount`, `sh:maxCount`
- `sh:hasValue` (for `rdf:type schema:Dataset`, `rdf:type schema:CreativeWork`)
- `sh:class` (for checking `about` points to a `Dataset`)

For profile-specific constraints, additional shapes graphs can be loaded at runtime. The compiler produces additional guards that are appended to the guard list. This is how domain-specific RO-Crate profiles add enforcement beyond the base spec.

**Post-merge validation (separate from pre-execution guards):**

After a CRDT merge (incoming batch application), the guards do NOT run as blocking validators. Instead, a separate `post_merge_check` function runs the reachability check and cardinality checks and returns a `Vec<CrateViolation>` without blocking the merge. These violations are stored in a dedicated `violations` keyspace in Fjall (key: `graph_id ++ violation_hash`, value: serialized `CrateViolation`) and surfaced through the API.

```rust
pub trait Guard: Send + Sync {
    fn check(
        &self,
        snapshot: &GraphSnapshot,
        delta: &[MaterializedQuadChange],
    ) -> Result<(), CrateViolation>;
}

pub struct ShaclCompiler;

impl ShaclCompiler {
    pub fn compile_shapes(shapes_ttl: &str) -> Result<Vec<Box<dyn Guard>>>;
}

pub fn pre_execution_validate(
    guards: &[Box<dyn Guard>],
    snapshot: &GraphSnapshot,
    delta: &[MaterializedQuadChange],
) -> Result<(), Vec<CrateViolation>>;

pub fn post_merge_check(
    snapshot: &GraphSnapshot,
    graph: &GraphId,
) -> Vec<CrateViolation>;
```

**Dependencies:** `oxrdf`, `aruna-core`, `aruna-rdf-store`.


### 1.5 `aruna-repl`

The replication layer. This crate contains the OR-Set CRDT merge logic, the local write pipeline, the incoming batch application pipeline, and the sync protocol.

**OR-Set merge semantics:**

The CRDT is a per-graph observed-remove set of quads. Each quad's liveness is tracked by its dot set in the `quad_dots` keyspace.

- **Add:** An add creates a new dot and adds it to the quad's dot set. If the quad already exists (dot set is non-empty), the new dot is appended. If the quad does not exist, a new entry is created in all quad indexes and the dot set is initialized with the single new dot.

- **Remove:** A remove records the frontier the removing actor had observed (the `witnessed` field). For each dot in the quad's current dot set, if that dot is within the witnessed frontier (i.e., `witnessed[dot.actor] >= dot.counter`), that dot is removed from the set. If the dot set becomes empty, the quad is removed from all indexes. If any dot remains (because it was added by an actor whose counter exceeds the witnessed frontier), the quad survives.

This gives add-wins behavior: a concurrent add from an actor the remover hasn't seen yet will survive the remove. This is exactly OR-Set / ORSWOT semantics.

**Local write pipeline:**

```
1. Client submits SPARQL Update (or direct quad operations)
2. aruna-sparql evaluates against snapshot, returns Vec<MaterializedQuadChange>
3. aruna-shacl pre-execution guards run against (snapshot + delta)
4. If guards fail: return Err(violations), abort
5. If guards pass:
   a. Begin Fjall WriteBatch
   b. Get next counter for (graph, actor) -> new Dot
   c. For each MaterializedQuadChange:
      - Insert: call store.insert_quad(batch, ..., dot)
      - Delete: call store.remove_quad(batch, ..., current_frontier)
   d. Build replication Batch with all QuadOps
   e. Update graph frontier (advance own actor's counter)
   f. Append Batch to batch_log
   g. Enqueue affected subjects in fts_queue
   h. Commit WriteBatch atomically
6. Send Batch to outbound channel (see aruna-sync)
```

**Incoming batch application:**

```
1. Receive Batch from inbound channel
2. Validate:
   a. Is the graph known? If not, create it.
   b. Is the batch's counter exactly one more than the last seen counter
      for this actor in this graph's frontier? If not:
      - If counter <= frontier[actor]: already seen, skip (idempotent)
      - If counter > frontier[actor] + 1: buffer for later (gap)
3. Apply each QuadOp:
   a. Add: add dot to quad's dot set, insert quad into indexes
      if dot set was empty
   b. Remove: remove witnessed dots from quad's dot set, remove
      quad from indexes if dot set becomes empty
4. Update graph frontier
5. Commit atomically
6. Run post_merge_check, store any violations
7. Enqueue affected subjects in fts_queue
```

**Gap handling:**

The prototype must handle out-of-order delivery within a single actor's batch stream. The simplest approach: maintain a per-graph, per-actor buffer (`BTreeMap<u64, Batch>`) for batches that arrive ahead of their expected sequence number. After applying a batch, check if the next expected counter is now available in the buffer and apply it too. Repeat until no more contiguous batches are available.

This is necessary because channels in the prototype are unbounded and unordered (we use separate channels per peer, but within a peer's outbound stream the ordering should be maintained by construction; the buffer handles the case where it isn't, as a safety net).

**API surface:**

```rust
pub struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    guards: Vec<Box<dyn Guard>>,
    actor: ActorId,
}

impl ReplicationEngine {
    pub fn local_update(
        &self,
        sparql_update: &str,
    ) -> Result<Batch, UpdateError>;

    pub fn local_insert_quads(
        &self,
        graph: &GraphId,
        quads: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
    ) -> Result<Batch, UpdateError>;

    pub fn apply_remote_batch(
        &self,
        batch: Batch,
    ) -> Result<MergeResult, MergeError>;

    pub fn batches_for_catchup(
        &self,
        graph: &GraphId,
        remote_frontier: &Frontier,
    ) -> Result<Vec<Batch>>;
}

pub struct MergeResult {
    pub applied: bool,
    pub violations: Vec<CrateViolation>,
}

pub enum UpdateError {
    SparqlError(String),
    ValidationFailed(Vec<CrateViolation>),
    StorageError(String),
}
```

**Dependencies:** `aruna-core`, `aruna-rdf-store`, `aruna-sparql`, `aruna-shacl`.


### 1.6 `aruna-search`

Tantivy sidecar index.

**Tantivy schema:**

```rust
let mut schema_builder = Schema::builder();
schema_builder.add_text_field("graph_id", STRING | STORED);
schema_builder.add_text_field("subject_id", STRING | STORED);
schema_builder.add_text_field("subject_iri", STRING | STORED);
schema_builder.add_text_field("rdf_types", TEXT | STORED);
schema_builder.add_text_field("name", TEXT | STORED);
schema_builder.add_text_field("description", TEXT | STORED);
schema_builder.add_text_field("keywords", TEXT | STORED);
schema_builder.add_text_field("all_text", TEXT); // concatenation for broad search
schema_builder.add_date_field("modified_at", INDEXED | STORED);
```

**Index unit:** One Tantivy document per `(graph_id, subject_id)` pair where the subject has at least one text-bearing predicate (`schema:name`, `schema:description`, `schema:keywords`, or any predicate in the configurable `PredicateFilter`).

**Reindex pipeline:**

A background worker (a spawned tokio task in the prototype) polls `GraphStore::drain_fts_queue`. For each `(graph_id, subject_id)`:

1. Query the RDF store for all triples with that subject in that graph.
2. Extract text-bearing predicate values.
3. Build a Tantivy document.
4. Delete any existing document with the same `(graph_id, subject_id)`.
5. Add the new document.
6. After processing a batch of queue items, commit the Tantivy index and reload the reader.

The worker runs in a loop with a configurable poll interval (default 100ms for the prototype). It processes up to 256 queue items per cycle.

**Query interface:**

```rust
pub struct SearchIndex {
    index: tantivy::Index,
    reader: tantivy::IndexReader,
    writer: Arc<Mutex<tantivy::IndexWriter>>,
    store: Arc<GraphStore>,
}

impl SearchIndex {
    pub fn open(path: impl AsRef<Path>, store: Arc<GraphStore>) -> Result<Self>;

    /// Full-text search, returns matching (graph_id, subject_iri) pairs
    pub fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>>;

    /// Search within a specific graph
    pub fn search_in_graph(
        &self,
        graph: &GraphId,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>>;

    /// Background reindex loop (call from a spawned task)
    pub async fn reindex_loop(&self, poll_interval: Duration);

    /// Force reindex of all resources in a graph (for rebuild/bootstrap)
    pub fn reindex_graph(&self, graph: &GraphId) -> Result<usize>;
}

pub struct SearchHit {
    pub graph_id: GraphId,
    pub subject_iri: String,
    pub score: f32,
    pub name: Option<String>,
    pub description: Option<String>,
}
```

**Two-phase query integration:**

For the prototype, the recommended query pattern is:

1. Run a Tantivy search to get `Vec<SearchHit>`.
2. Take the `subject_iri` values from the hits.
3. Construct a SPARQL query with a `VALUES` clause injecting the hit IRIs.
4. Run the SPARQL query to get full structured results.

This avoids any custom SPARQL function registration and is sufficient for the prototype.

**Dependencies:** `tantivy`, `aruna-core`, `aruna-rdf-store`.


### 1.7 `aruna-sync`

Channel-based synchronization layer that simulates a peer-to-peer network.

**Architecture:**

Each simulated peer is a `PeerNode` that owns its own `GraphStore`, `ReplicationEngine`, and `SearchIndex`. Peers communicate through `tokio::sync::mpsc` channels. The `SyncNetwork` struct manages the set of peers and the channel topology.

```rust
pub struct PeerNode {
    pub id: ActorId,
    pub store: Arc<GraphStore>,
    pub engine: Arc<ReplicationEngine>,
    pub search: Arc<SearchIndex>,
    outbound: mpsc::UnboundedSender<SyncMessage>,
    inbound: mpsc::UnboundedReceiver<SyncMessage>,
}

pub enum SyncMessage {
    /// A committed batch to replicate
    PushBatch(Batch),
    /// Request catch-up from a frontier
    CatchUpRequest {
        graph: GraphId,
        frontier: Frontier,
        reply_to: oneshot::Sender<Vec<Batch>>,
    },
    /// Bootstrap request: full graph snapshot
    SnapshotRequest {
        graph: GraphId,
        reply_to: oneshot::Sender<GraphSnapshot>,
    },
}

pub struct SyncNetwork {
    peers: Vec<PeerNode>,
    // Internal channel routing
}

impl SyncNetwork {
    pub fn new(peer_count: usize, data_dir: impl AsRef<Path>) -> Result<Self>;

    /// Get a mutable reference to a peer by index
    pub fn peer(&self, index: usize) -> &PeerNode;

    /// Deliver all pending messages (one round of sync)
    pub async fn sync_round(&mut self);

    /// Run sync until all peers have converged
    pub async fn sync_until_converged(&mut self, timeout: Duration) -> Result<()>;

    /// Partition: disconnect two peers (drop their channel, messages are lost)
    pub fn partition(&mut self, peer_a: usize, peer_b: usize);

    /// Heal: reconnect two peers and trigger catch-up
    pub async fn heal(&mut self, peer_a: usize, peer_b: usize);
}
```

**Channel topology:**

For N peers, each peer has a dedicated outbound sender and each other peer has the corresponding receiver. When peer A commits a batch, it sends the batch to all other peers' inbound channels. This is a full-mesh topology, which is fine for the 2-5 peers the prototype will simulate.

**Convergence detection:**

After `sync_until_converged`, the network checks that all peers have the same frontier for every graph. If frontiers match, it additionally verifies that the quad sets are identical by iterating all quads in each graph on each peer and comparing. Any mismatch is a convergence failure, which is a bug in the CRDT implementation.

**Dependencies:** `tokio`, `aruna-core`, `aruna-rdf-store`, `aruna-repl`, `aruna-search`.


### 1.8 `aruna-rocrate`: Update DO NOT implement this on your own, try to re-use as much as possible from the ro-crate-rs crate!

RO-Crate lifecycle management built on top of the other crates.

**Responsibilities:**

- Create a new RO-Crate by inserting the required root data entity, metadata descriptor, and mandatory properties into a new named graph.
- Import an existing RO-Crate from a JSON-LD file (the standard `ro-crate-metadata.json`) into a named graph, including blank node skolemization.
- Export a named graph back to `ro-crate-metadata.json` format.
- Provide convenience methods for common operations: add a data entity with `hasPart` linkage, add a contextual entity, update metadata properties.

**Blank node skolemization:**

When importing a JSON-LD document that contains blank nodes, each blank node is replaced with a deterministic IRI based on the graph's IRI and a hash of the blank node's local neighborhood. The neighborhood is defined as the sorted set of `(predicate, object)` pairs for outgoing edges and `(subject, predicate)` pairs for incoming edges, concatenated and hashed with BLAKE3. The resulting IRI is `{graph_iri}/.well-known/genid/{blake3_hex}`.

This ensures two independent imports of the same document produce the same skolemized IRIs, which prevents duplication after CRDT merge.

**Crate creation:**

```rust
pub struct RoCrateManager {
    engine: Arc<ReplicationEngine>,
}

impl RoCrateManager {
    /// Create a new RO-Crate with required base entities
    pub fn create_crate(
        &self,
        graph_id: GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: &str,
    ) -> Result<Batch>;

    /// Import from ro-crate-metadata.json content
    pub fn import_jsonld(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch>;

    /// Export a graph to ro-crate-metadata.json format
    pub fn export_jsonld(
        &self,
        graph_id: &GraphId,
    ) -> Result<String>;

    /// Add a data entity to a crate with proper hasPart linkage
    pub fn add_data_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> Result<Batch>;

    /// Add a contextual entity
    pub fn add_contextual_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> Result<Batch>;

    /// Update a property on an entity
    pub fn update_property(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        predicate: &str,
        old_value: Option<&str>,
        new_value: &str,
    ) -> Result<Batch>;
}
```

**Dependencies:** `json-ld` (or `sophia` for JSON-LD parsing), `blake3`, `oxrdf`, `aruna-core`, `aruna-repl`.


### 1.9 `aruna-tests`

Integration test crate that exercises the full stack through scenario-based tests.

This is not a library crate. It is a binary/test crate that sets up `SyncNetwork` instances, runs scenarios, and asserts correctness. The test scenarios are described in detail in section 3.

**Dependencies:** all other crates, `tokio`, `proptest` (for property-based testing).

---

## 2. Implementation Phases

### Phase 1: Foundation (estimated: 2-3 weeks)

**Goal:** A single-node Fjall-backed RDF store that can insert quads, query them by pattern, and create/query named graphs. No replication, no SPARQL, no search.

**Deliverables:**

- `aruna-core` with all type definitions.
- `aruna-rdf-store` with the term dictionary, all four quad indexes (`gspo`, `gpos`, `gosp`, `spog`), the `graphs` keyspace, and the `quads_for_pattern` method.
- Unit tests: insert 1000 quads, query by each single-component pattern, query by multi-component pattern, verify round-trip through term dictionary.
- Benchmark: insert throughput (quads/sec) and pattern query latency.

**Acceptance criteria:**

- All quad index orders return the same set of quads for unfiltered queries.
- Pattern queries return only matching quads.
- Term dictionary round-trips all RDF term types (named nodes, blank nodes, string literals, typed literals, language-tagged literals).

### Phase 2: SPARQL (estimated: 1-2 weeks)

**Goal:** SPARQL query and update evaluation on a single node.

**Deliverables:**

- `aruna-sparql` with query evaluation via `spareval` (or manual evaluator for a minimum viable subset).
- SPARQL Update support for `INSERT DATA`, `DELETE DATA`, and `DELETE/INSERT WHERE`.
- The update evaluator produces `Vec<MaterializedQuadChange>` without committing.

**Acceptance criteria:**

- `SELECT ?s ?p ?o WHERE { GRAPH <g1> { ?s ?p ?o } }` returns all quads in graph `g1`.
- `SELECT * WHERE { ?s schema:name ?name }` across all graphs returns correct results.
- `INSERT DATA { GRAPH <g1> { <e1> schema:name "test" } }` produces one `Insert` change.
- `DELETE { GRAPH <g1> { ?s schema:name ?old } } INSERT { GRAPH <g1> { ?s schema:name "new" } } WHERE { GRAPH <g1> { ?s schema:name ?old } }` produces matched Delete/Insert pairs.

### Phase 3: CRDT Replication (estimated: 2-3 weeks)

**Goal:** Multi-peer OR-Set replication with dotted causality over channels.

**Deliverables:**

- `aruna-repl` with the full local write pipeline and incoming batch application.
- `aruna-sync` with channel-based `SyncNetwork`.
- The `quad_dots`, `graph_frontier`, `actor_counter`, and `batch_log` keyspaces are populated and maintained.
- Convergence verification in `sync_until_converged`.

**Acceptance criteria (these are the hard tests):**

- **Add-add convergence:** Two peers independently add different quads to the same graph offline. After sync, both peers have both quads.
- **Add-remove with observed add:** Peer A adds a quad. Sync. Peer B observes the quad and removes it. Sync. Both peers agree the quad is gone.
- **Add-remove concurrent (add wins):** Peer A adds a quad. Concurrently (before sync), peer B removes the same quad (but B has never seen A's add). After sync, the quad survives because A's dot was not in B's witnessed frontier.
- **Remove after full observation:** Peer A adds a quad, syncs to B. B removes the quad (witnessed frontier includes A's dot). Sync. Both peers agree the quad is gone.
- **Duplicate batch replay (idempotence):** Deliver the same batch twice. The second application is a no-op. Frontier and quad state are identical before and after the duplicate.
- **Out-of-order delivery within actor:** Deliver batch (A, 3) before batch (A, 2). Batch (A, 3) is buffered. Then deliver (A, 2). Both are applied in order. State matches what sequential delivery would produce.
- **Three-peer diamond:** Peer A makes a change. It syncs to B and C independently. B and C both make concurrent changes. B syncs to A, C syncs to A. A converges. Then A syncs to B and C. All three converge.

### Phase 4: SHACL Guards (estimated: 1-2 weeks)

**Goal:** Pre-execution validation that prevents structurally invalid local commits, and post-merge violation detection.

**Deliverables:**

- `aruna-shacl` with hardcoded RO-Crate 1.2 base shapes.
- The six guards described in section 1.4.
- Integration with `aruna-repl`'s local write pipeline.
- `post_merge_check` function.
- Violations stored in Fjall and queryable.

**Acceptance criteria:**

- A local SPARQL update that deletes `<./> rdf:type schema:Dataset` is rejected.
- A local SPARQL update that removes the last `schema:name` from the root entity is rejected.
- A local SPARQL update that adds a new `File` entity without linking it via `hasPart` from the root is rejected.
- A local SPARQL update that adds a new entity without any `rdf:type` is rejected.
- After a merge that produces an orphaned data entity (peer A adds entity with hasPart link, peer B concurrently removes the hasPart link), `post_merge_check` returns an `OrphanedDataEntity` violation.
- A local SPARQL update that adds a second `datePublished` to the root entity is rejected.
- A normal metadata update (changing description, adding an author) passes all guards.

### Phase 5: RO-Crate Lifecycle (estimated: 1-2 weeks)

**Goal:** Create, import, export, and update RO-Crates through the `aruna-rocrate` API.

**Deliverables:**

- `aruna-rocrate` with create, import, export, and entity management.
- Blank node skolemization.
- JSON-LD export that produces valid `ro-crate-metadata.json`.

**Acceptance criteria:**

- `create_crate` produces a valid RO-Crate that passes all SHACL guards.
- `import_jsonld` on a real-world RO-Crate metadata file (e.g., from WorkflowHub or a Zenodo deposit) creates a named graph with all entities preserved.
- `export_jsonld` produces output that can be validated by an external RO-Crate validator (e.g., `ro-crate-html-js` or the Python `rocrate` library).
- Two independent imports of the same JSON-LD produce identical named graphs (blank node skolemization is deterministic).
- `add_data_entity` creates the entity and links it via `hasPart`, passing SHACL guards.

### Phase 6: Search (estimated: 1 week)

**Goal:** Tantivy sidecar index with background reindexing and two-phase query.

**Deliverables:**

- `aruna-search` with the schema, reindex pipeline, and query interface.
- Integration with `aruna-repl` (FTS queue population on commit).
- Background reindex worker.

**Acceptance criteria:**

- After inserting a crate with `schema:name "Genomic Analysis of E. coli"`, searching for `"genomic"` returns the crate's root entity.
- After updating a name from `"Genomic Analysis"` to `"Proteomic Analysis"`, searching for `"genomic"` returns nothing, searching for `"proteomic"` returns the entity.
- Search works across multiple crates.
- Search within a specific graph returns only results from that graph.
- Two-phase query: search for `"genomic"` -> get subject IRI -> SPARQL query for all properties of that entity -> returns full entity metadata.

### Phase 7: Integration Scenarios (estimated: 1-2 weeks)

**Goal:** End-to-end scenarios that exercise the full stack together.

**Deliverables:**

- `aruna-tests` with the scenario tests described in section 3.
- Property-based tests for CRDT convergence.
- Documentation of all known limitations and the path to production.

---

## 3. Test Scenarios

### 3.1 Scenario: Concurrent Metadata Editing

Two peers independently edit different metadata fields on the same crate.

Setup: Create a crate on peer 0 with name "Original Dataset", description "Original description". Sync to peer 1.

Actions:
- Peer 0 (offline): Updates name to "Updated Dataset v2".
- Peer 1 (offline): Updates description to "Improved description with more detail".

Sync and verify:
- Both peers converge to a state with name "Updated Dataset v2" and description "Improved description with more detail".
- No violations.
- Tantivy search on both peers finds the crate for both "Updated" and "Improved".

### 3.2 Scenario: Concurrent Same-Field Editing (Two Titles)

Two peers independently update the same field.

Setup: Create a crate on peer 0 with name "Original". Sync to peer 1.

Actions:
- Peer 0 (offline): `DELETE { <./> schema:name "Original" } INSERT { <./> schema:name "Peer 0 Title" } WHERE { <./> schema:name "Original" }`
- Peer 1 (offline): `DELETE { <./> schema:name "Original" } INSERT { <./> schema:name "Peer 1 Title" } WHERE { <./> schema:name "Original" }`

Sync and verify:
- The `DELETE` of "Original" succeeds on both sides (both observed it).
- Both adds survive (concurrent adds from different actors).
- The crate now has two `schema:name` values: "Peer 0 Title" and "Peer 1 Title".
- This is valid RO-Crate (name has no max cardinality constraint other than "MUST be present").
- No violations from post-merge check.
- The application layer can surface "this entity has two names" as a cleanup task.

### 3.3 Scenario: Concurrent Entity Addition

Two peers independently add different data entities to the same crate.

Setup: Create a crate on peer 0. Sync to peer 1.

Actions:
- Peer 0 (offline): Add file entity `results.csv` with hasPart link from root.
- Peer 1 (offline): Add file entity `analysis.py` with hasPart link from root.

Sync and verify:
- Both entities exist in the merged graph.
- Both hasPart links exist.
- Both entities are reachable from root.
- No violations.

### 3.4 Scenario: Orphaned Entity After Concurrent Edit

One peer adds an entity, another reorganizes the folder structure.

Setup: Create a crate on peer 0 with a Dataset entity `data/` linked via hasPart from root. Sync to peer 1.

Actions:
- Peer 0 (offline): Add file entity `data/results.csv` with hasPart link from `data/` (not from root).
- Peer 1 (offline): Remove the hasPart link from root to `data/` and remove `data/` entity.

Sync and verify:
- Peer 1's remove of the hasPart triple (root -> data/) succeeds because it observed the add.
- Peer 0's add of `data/results.csv` and the hasPart link (data/ -> data/results.csv) survive because peer 1 never observed them.
- Result: `data/results.csv` exists and `data/` exists (the entity triples survive because peer 1's remove of the entity was concurrent with peer 0's add of a hasPart to it), but neither is reachable from root.
- `post_merge_check` returns `OrphanedDataEntity` violations for both.
- The user must re-link them or delete them.

### 3.5 Scenario: SHACL Guard Prevents Root Destruction

A peer tries to delete the root data entity locally.

Actions:
- Peer 0: `DELETE DATA { GRAPH <crate1> { <./> rdf:type schema:Dataset } }`

Verify:
- The update is rejected with `CrateViolation::MissingRootDataEntity`.
- No batch is produced, no replication occurs.
- The graph is unchanged.

### 3.6 Scenario: SHACL Guard Prevents Orphan Creation

A peer tries to add a data entity without hasPart linkage.

Actions:
- Peer 0: INSERT DATA with a new File entity but no hasPart triple from root.

Verify:
- The update is rejected with `CrateViolation::OrphanedDataEntity`.
- Using `add_data_entity` from `aruna-rocrate` instead succeeds because it automatically adds the hasPart link.

### 3.7 Scenario: Three-Peer Convergence Under Partition

Setup: Three peers, all start with the same crate.

Actions:
- Network partition: peer 2 is isolated from peers 0 and 1.
- Peer 0 adds entity A with hasPart link. Syncs to peer 1.
- Peer 1 adds entity B with hasPart link. Syncs to peer 0.
- Peer 2 (isolated) adds entity C with hasPart link and modifies the description.
- Heal partition.
- Full sync.

Verify:
- All three peers converge to the same state: entities A, B, C all present, all linked via hasPart, description updated to peer 2's version (peer 2's add wins because the original description was also observed by 0 and 1 who didn't modify it).
- Wait, actually: if peer 0 and 1 did not modify description, then only peer 2's delete of the old description and insert of the new one is in the log. After merge, the old description's dots are witnessed by peer 2's remove, so it disappears. The new description's dot (from peer 2) survives. All peers have the new description. Correct.

### 3.8 Scenario: Search After Concurrent Edits

Setup: Crate with name "Microbial Genomics Study". Synced to all peers.

Actions:
- Peer 0 (offline): Changes name to "Microbial Proteomics Study".
- Peer 1 (offline): Adds entity with description containing "metagenomic assembly".

Sync and verify:
- Name field has two values after merge: "Microbial Proteomics Study" (from peer 0) and "Microbial Genomics Study" (if peer 1's delete of the old name was concurrent with peer 0's delete, both deletes observed the original, so the original is gone; peer 0's new value and... wait).
- Let's be precise. Peer 0 does DELETE "Microbial Genomics Study" / INSERT "Microbial Proteomics Study". Peer 1 does not touch the name. After merge: the original name was removed by peer 0 (observed). Peer 0's new name was added. Peer 1 made no changes to the name. Result: one name value, "Microbial Proteomics Study".
- Search for "proteomics" finds the root entity.
- Search for "metagenomic" finds the new entity added by peer 1.
- Search for "genomics" finds nothing (old name removed, and the word doesn't appear elsewhere).

### 3.9 Scenario: Property-Based CRDT Convergence Test

Use `proptest` to generate random sequences of operations on two or three peers, with random sync points and partitions.

**Operation generators:**

- `AddQuad(peer, graph, s, p, o)`: add a random quad.
- `RemoveQuad(peer, graph, s, p, o)`: remove a quad that the peer currently has.
- `SyncPair(peer_a, peer_b)`: sync all pending batches between two peers.
- `Partition(peer_a, peer_b)`: disconnect two peers.
- `Heal(peer_a, peer_b)`: reconnect two peers.

**Invariant to verify:** After a final `sync_until_converged`, all peers have identical quad sets for every graph, and all peers have identical frontiers.

**Shrinking:** `proptest` will automatically shrink failing cases to minimal reproduction sequences.

### 3.10 Scenario: Snapshot Bootstrap

A new peer joins after significant activity.

Setup: Peers 0 and 1 have been editing a crate with 50+ batches. Peer 2 is new.

Actions:
- Peer 2 requests a snapshot of the crate from peer 0.
- Peer 0 sends the current graph state plus the current frontier.
- Peer 2 loads the snapshot.
- Peer 1 sends a new batch while the snapshot transfer is in progress.
- Peer 2 receives the new batch and applies it via normal catch-up (its frontier is the snapshot frontier, the batch is beyond that frontier).

Verify:
- Peer 2's state matches peers 0 and 1 after sync.
- The snapshot frontier correctly enables incremental catch-up.

### 3.11 Scenario: Full RO-Crate Lifecycle

End-to-end test that exercises the entire stack.

Actions:
1. Peer 0 creates a new crate with `create_crate("Experiment Results", "Data from experiment X", "2025-01-15", "https://creativecommons.org/licenses/by/4.0/")`.
2. Peer 0 adds data entities: `data/sample1.fastq`, `data/sample2.fastq`, `analysis/pipeline.nf`.
3. Peer 0 adds contextual entities: author (Person with ORCID), organization, funding grant.
4. Sync to peer 1.
5. Peer 1 adds a data entity: `results/report.pdf`.
6. Peer 1 updates the description.
7. Peer 0 (offline) adds `results/figures/fig1.png` nested under a new `results/figures/` Dataset.
8. Sync all.
9. Export from peer 1 to JSON-LD.
10. Validate the exported JSON-LD with an external validator (or at minimum, parse it back and verify all entities are present).
11. Search for "experiment" on peer 0, verify root entity is found.
12. Search for "report" on peer 1, verify `results/report.pdf` is found.

---

## 4. Technical Decisions and Tradeoffs

### 4.1 Why not fork Oxigraph directly

Forking the Oxigraph repository and replacing RocksDB with Fjall inside the existing codebase would give us the full Oxigraph SPARQL implementation, but it would also give us a massive surface area to maintain and a tight coupling to Oxigraph's internal release cycle. The prototype takes the narrower path: depend on the published crate ecosystem (`oxrdf`, `spargebra`, `spareval`) and build a new store that satisfies the evaluator's expectations without carrying Oxigraph's full store implementation.

If the prototype validates the approach, a production version could reconsider a proper fork. But for the prototype, speed of iteration matters more than SPARQL completeness.

### 4.2 Why four quad indexes instead of nine

Oxigraph's production store maintains nine index orders. The prototype keeps four: `gspo`, `gpos`, `gosp` (for within-graph queries, which dominate the RO-Crate use case) and `spog` (for cross-graph queries like "find all crates by author X"). This is the minimum needed for the query patterns the prototype exercises. Adding more indexes later is a data migration, but for the prototype it is sufficient.

### 4.3 Why channel-based sync instead of network

The prototype's purpose is to validate merge semantics, not network protocol behavior. Channels give us deterministic control over message ordering, partitioning, and delivery timing, which is essential for writing precise CRDT convergence tests. They also eliminate an entire category of bugs (TCP framing, reconnection, authentication) that would slow down iteration without testing anything the prototype cares about.

The sync layer is designed so that replacing channels with a network transport (e.g., iroh-gossip, QUIC, or plain TCP) requires changing only the `SyncMessage` send/receive code in `aruna-sync`, not any of the replication logic in `aruna-repl`.

### 4.4 Why witnessed frontier on Remove instead of per-quad witnessed dots

The architecture document raised the question of whether `Del` operations should carry the full graph frontier or per-quad witnessed dots. The prototype uses the full graph frontier because it is simpler to implement and reason about. The trade-off is that it is coarser-grained: a remove "witnesses" all dots up to the frontier, including dots on unrelated quads. For the prototype's small graphs this does not matter. A production version could switch to per-quad witnessed dots if profiling shows false "this add was witnessed" cases causing unexpected quad loss.

### 4.5 Why SHACL compilation rather than runtime evaluation

A general-purpose SHACL engine (like `shacl_validation` or TopQuadrant's engine) would evaluate shapes against the full graph on every update. For a graph with thousands of triples, this is expensive. The compiled approach turns each shape into a targeted check that inspects only the quads affected by the current delta. This is cheap enough to run on every local commit without measurable latency impact.

The compilation is also a forcing function for understanding exactly which constraints the system enforces. A general SHACL engine could silently accept shapes that the system isn't prepared to handle during merge. The compiler either supports a shape kind or rejects it at startup, making the enforcement boundary explicit.

### 4.6 Why post-merge violations are stored, not blocked

Blocking a merge based on SHACL violations would break CRDT convergence. If peer A and peer B both apply valid local updates, but the merged result violates a constraint (e.g., orphaned entity from concurrent restructuring), blocking the merge on one peer would cause the peers to diverge permanently. The CRDT guarantee requires that all peers accept the same batches and converge to the same state. Violations are surfaced for resolution, not used as merge gates.

---

## 5. Dependency Summary

```toml
[workspace.dependencies]
# RDF ecosystem
oxrdf = "0.2"
spargebra = "0.3"
spareval = "0.1"

# Storage
fjall = "2"
postcard = { version = "1", features = ["alloc"] }

# Search
tantivy = "0.22"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Async runtime
tokio = { version = "1", features = ["full"] }

# Identity
uuid = { version = "1", features = ["v4", "serde"] }

# Time
chrono = { version = "0.4", features = ["serde"] }

# JSON-LD (for RO-Crate import/export)
json-ld = "0.21"

# Hashing (for blank node skolemization)
blake3 = "1"

# Testing
proptest = "1"
tempfile = "3"
```

Version numbers should be pinned to exact versions in the prototype to avoid surprise breakage. Run `cargo update` deliberately, not automatically.

---

## 6. Open Questions for Prototype Resolution

These are questions that the prototype should answer through implementation experience, not upfront design:

1. **spareval integration cost.** How much adapter code is needed to satisfy `spareval`'s store interface? If it is more than ~500 lines, the prototype should fall back to a manual evaluator for the SPARQL subset it needs.

2. **Fjall cross-keyspace atomicity under high write load.** Fjall documents cross-keyspace atomic writes. Does this hold under concurrent readers during a write? The prototype should include a stress test with concurrent reads and writes on the same graph.

3. **Tantivy reindex latency under merge storms.** When a peer receives 50 batches at once during catch-up, the FTS queue fills up. How long does it take for Tantivy to catch up? Is the 100ms poll interval sufficient? The prototype should measure this and document the result.

4. **postcard encoding stability.** The prototype uses postcard for serializing terms and CRDT metadata in Fjall. Postcard's encoding is not self-describing, so any struct layout change requires a migration. The prototype should document the exact struct layouts used and flag any place where a layout change would require re-encoding existing data.

5. **Memory pressure from buffered out-of-order batches.** The gap buffer in `aruna-repl` holds batches in memory until their predecessor arrives. For the prototype's small graphs this is fine. At what scale does it become a problem? The prototype should track buffer depth as a metric.

6. **SHACL guard interaction with blank node skolemization.** If an imported JSON-LD file has a blank node as a data entity, and skolemization produces a named node IRI, does the hasPart reachability check correctly trace through the skolemized IRI? This needs an explicit test case.