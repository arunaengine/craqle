use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::*;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("term not found: {0}")]
    TermNotFound(u64),
    #[error("graph not found: {0}")]
    GraphNotFound(String),
    #[error("snapshot import failed: {0}")]
    SnapshotImport(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Compact integer ID for an RDF term within the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TermId(pub u64);

impl TermId {
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub fn from_be_bytes(b: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(b))
    }
}

/// A quad represented by term IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedQuad {
    pub graph: TermId,
    pub subject: TermId,
    pub predicate: TermId,
    pub object: TermId,
}

/// Fjall v3 write batch type alias.
pub type WriteBatch = fjall::OwnedWriteBatch;

/// Fjall-backed RDF named-graph store with CRDT support structures.
pub struct GraphStore {
    db: Database,
    term2id: Keyspace,
    id2term: Keyspace,
    next_id: AtomicU64,
    term_lock: Mutex<()>,
    gspo: Keyspace,
    gsp_count: Keyspace,
    gpos: Keyspace,
    gosp: Keyspace,
    spog: Keyspace,
    graphs: Keyspace,
    graph_frontier: Keyspace,
    actor_counter: Keyspace,
    quad_dots: Keyspace,
    batch_log: Keyspace,
    fts_queue: Keyspace,
    fts_counter: AtomicU64,
}

/// Helper: iterate a keyspace prefix and collect (key, value) pairs.
fn collect_prefix(ks: &Keyspace, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for guard in ks.prefix(prefix) {
        let (k, v) = guard.into_inner()?;
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}

fn collect_iter(ks: &Keyspace) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for guard in ks.iter() {
        let (k, v) = guard.into_inner()?;
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}

fn next_lexicographic_key(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    for idx in (0..next.len()).rev() {
        if next[idx] != u8::MAX {
            next[idx] += 1;
            next.truncate(idx + 1);
            return Some(next);
        }
    }
    None
}

impl GraphStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let opts = KeyspaceCreateOptions::default;

        let term2id = db.keyspace("term2id", opts)?;
        let id2term = db.keyspace("id2term", opts)?;
        let gspo = db.keyspace("gspo", opts)?;
        let gsp_count = db.keyspace("gsp_count", opts)?;
        let gpos = db.keyspace("gpos", opts)?;
        let gosp = db.keyspace("gosp", opts)?;
        let spog = db.keyspace("spog", opts)?;
        let graphs = db.keyspace("graphs", opts)?;
        let graph_frontier = db.keyspace("graph_frontier", opts)?;
        let actor_counter = db.keyspace("actor_counter", opts)?;
        let quad_dots = db.keyspace("quad_dots", opts)?;
        let batch_log = db.keyspace("batch_log", opts)?;
        let fts_queue = db.keyspace("fts_queue", opts)?;

        let next_id = match term2id.get("__next_id")? {
            Some(v) => {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                AtomicU64::new(u64::from_be_bytes(bytes))
            }
            None => AtomicU64::new(1),
        };

        let fts_counter = match fts_queue.get("__next_fts")? {
            Some(v) => {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                AtomicU64::new(u64::from_be_bytes(bytes))
            }
            None => AtomicU64::new(1),
        };

        Ok(Self {
            db,
            term2id,
            id2term,
            next_id,
            term_lock: Mutex::new(()),
            gspo,
            gsp_count,
            gpos,
            gosp,
            spog,
            graphs,
            graph_frontier,
            actor_counter,
            quad_dots,
            batch_log,
            fts_queue,
            fts_counter,
        })
    }

    // ── Term Dictionary ─────────────────────────────────────────────────

    pub fn encode_term(&self, term: &EncodedTerm) -> Result<TermId> {
        let key = term.0.as_bytes();
        // Fast path: check without lock
        if let Some(v) = self.term2id.get(key)? {
            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
            return Ok(TermId::from_be_bytes(bytes));
        }
        // Slow path: hold lock to prevent TOCTOU race
        let _guard = self.term_lock.lock().unwrap();
        // Re-check under lock (another thread may have inserted)
        if let Some(v) = self.term2id.get(key)? {
            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
            return Ok(TermId::from_be_bytes(bytes));
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let id_bytes = id.to_be_bytes();
        let mut batch = self.db.batch();
        batch.insert(&self.term2id, key, &id_bytes);
        batch.insert(&self.id2term, &id_bytes, key);
        batch.insert(&self.term2id, "__next_id", &(id + 1).to_be_bytes());
        batch.commit()?;
        Ok(TermId(id))
    }

    pub fn decode_term(&self, id: TermId) -> Result<EncodedTerm> {
        match self.id2term.get(id.to_be_bytes())? {
            Some(v) => Ok(EncodedTerm(
                String::from_utf8_lossy(v.as_ref()).into_owned(),
            )),
            None => Err(StoreError::TermNotFound(id.0)),
        }
    }

    pub fn lookup_term(&self, term: &EncodedTerm) -> Result<Option<TermId>> {
        match self.term2id.get(term.0.as_bytes())? {
            Some(v) => {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                Ok(Some(TermId::from_be_bytes(bytes)))
            }
            None => Ok(None),
        }
    }

    /// Encode a term, creating a new dictionary entry if it doesn't exist.
    /// Alias for `encode_term` — use `lookup_term` for read-only lookups.
    pub fn resolve_term(&self, term: &EncodedTerm) -> Result<TermId> {
        self.encode_term(term)
    }

    // ── Graph Lifecycle ─────────────────────────────────────────────────

    pub fn create_graph(&self, graph: &GraphId) -> Result<()> {
        let gid = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        self.graphs.insert(gid.to_be_bytes(), b"")?;
        Ok(())
    }

    pub fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        let gid = match self.lookup_term(&EncodedTerm::from_named_node(&graph.0))? {
            Some(id) => id,
            None => return Ok(false),
        };
        Ok(self.graphs.get(gid.to_be_bytes())?.is_some())
    }

    pub fn graphs(&self) -> Result<Vec<GraphId>> {
        let mut result = Vec::new();
        for item in collect_iter(&self.graphs)? {
            let (k, _) = item;
            if k.len() == 8 {
                let tid = TermId::from_be_bytes(k.try_into().unwrap());
                let term = self.decode_term(tid)?;
                if let Some(nn) = term.to_named_node() {
                    result.push(GraphId(nn));
                }
            }
        }
        Ok(result)
    }

    pub fn graph_snapshot(&self, graph: &GraphId) -> Result<GraphReplicaSnapshot> {
        let frontier = self.get_frontier(graph)?;
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = self.lookup_term(&graph_term)? else {
            return Ok(GraphReplicaSnapshot {
                graph: graph.clone(),
                frontier,
                quads: Vec::new(),
            });
        };

        let quads = self
            .quads_for_pattern(Some(graph_id), None, None, None)?
            .into_iter()
            .map(|quad| {
                let key = Self::quad_key(graph_id, quad.subject, quad.predicate, quad.object);
                Ok(SnapshotQuadState {
                    subject: self.decode_term(quad.subject)?,
                    predicate: self.decode_term(quad.predicate)?,
                    object: self.decode_term(quad.object)?,
                    dots: self.get_dots(&key)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            frontier,
            quads,
        })
    }

    pub fn import_graph_snapshot(&self, snapshot: &GraphReplicaSnapshot) -> Result<()> {
        if self.contains_graph(&snapshot.graph)? {
            let existing = self.graph_snapshot(&snapshot.graph)?;
            if !existing.quads.is_empty() || !existing.frontier.0.is_empty() {
                return Err(StoreError::SnapshotImport(format!(
                    "graph `{}` already contains data",
                    snapshot.graph.as_str()
                )));
            }
        } else {
            self.create_graph(&snapshot.graph)?;
        }

        let mut batch = self.new_batch();
        let graph_id = self.resolve_term(&EncodedTerm::from_named_node(&snapshot.graph.0))?;
        let mut affected_subjects = std::collections::HashSet::new();
        let mut count_deltas = HashMap::new();

        for quad in &snapshot.quads {
            let subject = self.resolve_term(&quad.subject)?;
            let predicate = self.resolve_term(&quad.predicate)?;
            let object = self.resolve_term(&quad.object)?;
            let gspo = Self::quad_key(graph_id, subject, predicate, object);
            let gpos = Self::quad_key(graph_id, predicate, object, subject);
            let gosp = Self::quad_key(graph_id, object, subject, predicate);
            let spog = Self::quad_key(subject, predicate, object, graph_id);

            batch.insert(&self.gspo, gspo, b"");
            batch.insert(&self.gpos, gpos, b"");
            batch.insert(&self.gosp, gosp, b"");
            batch.insert(&self.spog, spog, b"");
            batch.insert(&self.quad_dots, gspo, &postcard::to_allocvec(&quad.dots)?);
            affected_subjects.insert(subject);
            *count_deltas
                .entry((graph_id, subject, predicate))
                .or_insert(0i64) += 1;
        }

        self.apply_subject_predicate_count_deltas(&mut batch, &count_deltas)?;

        for subject in affected_subjects {
            self.enqueue_fts(&mut batch, &snapshot.graph, subject)?;
        }

        self.set_frontier(&mut batch, &snapshot.graph, &snapshot.frontier)?;
        self.commit(batch)
    }

    pub fn graph_fingerprint(&self, graph: &GraphId) -> Result<(u64, [u8; 32], [u8; 32])> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = self.lookup_term(&graph_term)? else {
            let empty = *blake3::hash(&[]).as_bytes();
            return Ok((0, empty, empty));
        };

        let prefix = graph_id.to_be_bytes();
        let mut count = 0u64;
        let mut xor = [0u8; 32];
        let mut sum = [0u8; 32];
        for guard in self.gspo.prefix(&prefix) {
            let (key, _) = guard.into_inner()?;
            if key.len() < 32 {
                continue;
            }
            let subject = TermId::from_be_bytes(key[8..16].try_into().unwrap());
            let predicate = TermId::from_be_bytes(key[16..24].try_into().unwrap());
            let object = TermId::from_be_bytes(key[24..32].try_into().unwrap());
            let mut hasher = blake3::Hasher::new();
            hasher.update(self.decode_term(subject)?.0.as_bytes());
            hasher.update(&[0]);
            hasher.update(self.decode_term(predicate)?.0.as_bytes());
            hasher.update(&[0]);
            hasher.update(self.decode_term(object)?.0.as_bytes());
            let quad_hash = hasher.finalize();
            for (idx, byte) in quad_hash.as_bytes().iter().enumerate() {
                xor[idx] ^= byte;
                sum[idx] = sum[idx].wrapping_add(*byte);
            }
            count += 1;
        }

        Ok((count, xor, sum))
    }

    // ── Quad Operations ─────────────────────────────────────────────────

    fn quad_key(a: TermId, b: TermId, c: TermId, d: TermId) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&a.to_be_bytes());
        key[8..16].copy_from_slice(&b.to_be_bytes());
        key[16..24].copy_from_slice(&c.to_be_bytes());
        key[24..32].copy_from_slice(&d.to_be_bytes());
        key
    }

    fn subject_prefix(graph: TermId, subject: TermId) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[0..8].copy_from_slice(&graph.to_be_bytes());
        key[8..16].copy_from_slice(&subject.to_be_bytes());
        key
    }

    fn subject_predicate_key(graph: TermId, subject: TermId, predicate: TermId) -> [u8; 24] {
        let mut key = [0u8; 24];
        key[0..8].copy_from_slice(&graph.to_be_bytes());
        key[8..16].copy_from_slice(&subject.to_be_bytes());
        key[16..24].copy_from_slice(&predicate.to_be_bytes());
        key
    }

    fn subject_predicate_count_by_ids(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
    ) -> Result<usize> {
        let key = Self::subject_predicate_key(graph, subject, predicate);
        if let Some(value) = self.gsp_count.get(key)? {
            let bytes: [u8; 8] = value.as_ref().try_into().unwrap_or([0; 8]);
            return Ok(u64::from_be_bytes(bytes) as usize);
        }

        let prefix = key;
        let mut total = 0usize;
        for guard in self.gspo.prefix(&prefix) {
            let _ = guard.into_inner()?;
            total += 1;
        }
        Ok(total)
    }

    pub fn apply_subject_predicate_count_deltas(
        &self,
        batch: &mut WriteBatch,
        deltas: &HashMap<(TermId, TermId, TermId), i64>,
    ) -> Result<()> {
        for (&(graph, subject, predicate), &delta) in deltas {
            if delta == 0 {
                continue;
            }

            let key = Self::subject_predicate_key(graph, subject, predicate);
            let current = self.subject_predicate_count_by_ids(graph, subject, predicate)? as i64;
            let updated = current + delta;
            if updated <= 0 {
                batch.remove(&self.gsp_count, key);
            } else {
                batch.insert(&self.gsp_count, key, &(updated as u64).to_be_bytes());
            }
        }
        Ok(())
    }

    fn collect_triples_in_range<R>(
        &self,
        range: R,
        triples: &mut Vec<(EncodedTerm, EncodedTerm)>,
    ) -> Result<()>
    where
        R: std::ops::RangeBounds<Vec<u8>>,
    {
        for guard in self.gspo.range(range) {
            let (key, _) = guard.into_inner()?;
            if key.len() < 32 {
                continue;
            }
            let predicate = TermId::from_be_bytes(key[16..24].try_into().unwrap());
            let object = TermId::from_be_bytes(key[24..32].try_into().unwrap());
            triples.push((self.decode_term(predicate)?, self.decode_term(object)?));
        }
        Ok(())
    }

    pub fn insert_quad(
        &self,
        batch: &mut WriteBatch,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        dot: &Dot,
    ) -> Result<bool> {
        let gspo = Self::quad_key(graph, subject, predicate, object);
        let gpos = Self::quad_key(graph, predicate, object, subject);
        let gosp = Self::quad_key(graph, object, subject, predicate);
        let spog = Self::quad_key(subject, predicate, object, graph);

        batch.insert(&self.gspo, gspo, b"");
        batch.insert(&self.gpos, gpos, b"");
        batch.insert(&self.gosp, gosp, b"");
        batch.insert(&self.spog, spog, b"");

        let mut dots = self.get_dots(&gspo)?;
        let inserted_new_live_quad = dots.is_empty();
        dots.push(*dot);
        batch.insert(&self.quad_dots, gspo, &postcard::to_allocvec(&dots)?);
        Ok(inserted_new_live_quad)
    }

    pub fn remove_quad(
        &self,
        batch: &mut WriteBatch,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        witnessed: &Frontier,
    ) -> Result<bool> {
        let gspo = Self::quad_key(graph, subject, predicate, object);
        let mut dots = self.get_dots(&gspo)?;
        if dots.is_empty() {
            return Ok(false);
        }

        dots.retain(|d| !witnessed.contains(d));

        if dots.is_empty() {
            let gpos = Self::quad_key(graph, predicate, object, subject);
            let gosp = Self::quad_key(graph, object, subject, predicate);
            let spog = Self::quad_key(subject, predicate, object, graph);
            batch.remove(&self.gspo, gspo);
            batch.remove(&self.gpos, gpos);
            batch.remove(&self.gosp, gosp);
            batch.remove(&self.spog, spog);
            batch.remove(&self.quad_dots, gspo);
            Ok(true)
        } else {
            batch.insert(&self.quad_dots, gspo, &postcard::to_allocvec(&dots)?);
            Ok(false)
        }
    }

    fn get_dots(&self, key: &[u8; 32]) -> Result<Vec<Dot>> {
        match self.quad_dots.get(key)? {
            Some(v) => Ok(postcard::from_bytes(v.as_ref())?),
            None => Ok(Vec::new()),
        }
    }

    // ── Quad Queries ────────────────────────────────────────────────────

    pub fn quads_for_pattern(
        &self,
        graph: Option<TermId>,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        match (graph, subject, predicate, object) {
            (Some(g), Some(s), Some(p), Some(o)) => {
                let key = Self::quad_key(g, s, p, o);
                if self.gspo.get(key)?.is_some() {
                    Ok(vec![EncodedQuad {
                        graph: g,
                        subject: s,
                        predicate: p,
                        object: o,
                    }])
                } else {
                    Ok(vec![])
                }
            }
            // Route to the optimal index based on which components are bound
            (Some(g), Some(s), p, o) => self.scan_gspo(g, Some(s), p, o),
            (Some(g), None, Some(p), o) => self.scan_gpos(g, p, o),
            (Some(g), None, None, Some(o)) => self.scan_gosp(g, o),
            (Some(g), None, None, None) => self.scan_gspo(g, None, None, None),
            (None, s, p, o) => self.scan_spog(s, p, o),
        }
    }

    fn scan_gspo(
        &self,
        g: TermId,
        s: Option<TermId>,
        p: Option<TermId>,
        o: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        let prefix = match (s, p) {
            (Some(s), Some(p)) => {
                let mut v = Vec::with_capacity(24);
                v.extend_from_slice(&g.to_be_bytes());
                v.extend_from_slice(&s.to_be_bytes());
                v.extend_from_slice(&p.to_be_bytes());
                v
            }
            (Some(s), None) => {
                let mut v = Vec::with_capacity(16);
                v.extend_from_slice(&g.to_be_bytes());
                v.extend_from_slice(&s.to_be_bytes());
                v
            }
            _ => g.to_be_bytes().to_vec(),
        };

        let mut quads = Vec::new();
        for (k, _) in collect_prefix(&self.gspo, &prefix)? {
            if k.len() < 32 {
                continue;
            }
            let q = EncodedQuad {
                graph: TermId::from_be_bytes(k[0..8].try_into().unwrap()),
                subject: TermId::from_be_bytes(k[8..16].try_into().unwrap()),
                predicate: TermId::from_be_bytes(k[16..24].try_into().unwrap()),
                object: TermId::from_be_bytes(k[24..32].try_into().unwrap()),
            };
            if o.is_some_and(|f| f != q.object) {
                continue;
            }
            if p.is_some_and(|f| f != q.predicate) {
                continue;
            }
            quads.push(q);
        }
        Ok(quads)
    }

    /// Scan gpos index: key layout is [graph, predicate, object, subject].
    fn scan_gpos(&self, g: TermId, p: TermId, o: Option<TermId>) -> Result<Vec<EncodedQuad>> {
        let mut prefix = Vec::with_capacity(24);
        prefix.extend_from_slice(&g.to_be_bytes());
        prefix.extend_from_slice(&p.to_be_bytes());
        if let Some(o) = o {
            prefix.extend_from_slice(&o.to_be_bytes());
        }

        let mut quads = Vec::new();
        for (k, _) in collect_prefix(&self.gpos, &prefix)? {
            if k.len() < 32 {
                continue;
            }
            quads.push(EncodedQuad {
                graph: TermId::from_be_bytes(k[0..8].try_into().unwrap()),
                predicate: TermId::from_be_bytes(k[8..16].try_into().unwrap()),
                object: TermId::from_be_bytes(k[16..24].try_into().unwrap()),
                subject: TermId::from_be_bytes(k[24..32].try_into().unwrap()),
            });
        }
        Ok(quads)
    }

    /// Scan gosp index: key layout is [graph, object, subject, predicate].
    fn scan_gosp(&self, g: TermId, o: TermId) -> Result<Vec<EncodedQuad>> {
        let mut prefix = Vec::with_capacity(16);
        prefix.extend_from_slice(&g.to_be_bytes());
        prefix.extend_from_slice(&o.to_be_bytes());

        let mut quads = Vec::new();
        for (k, _) in collect_prefix(&self.gosp, &prefix)? {
            if k.len() < 32 {
                continue;
            }
            quads.push(EncodedQuad {
                graph: TermId::from_be_bytes(k[0..8].try_into().unwrap()),
                object: TermId::from_be_bytes(k[8..16].try_into().unwrap()),
                subject: TermId::from_be_bytes(k[16..24].try_into().unwrap()),
                predicate: TermId::from_be_bytes(k[24..32].try_into().unwrap()),
            });
        }
        Ok(quads)
    }

    fn scan_spog(
        &self,
        s: Option<TermId>,
        p: Option<TermId>,
        o: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        let prefix = match (s, p, o) {
            (Some(s), Some(p), Some(o)) => {
                let mut v = Vec::with_capacity(24);
                v.extend_from_slice(&s.to_be_bytes());
                v.extend_from_slice(&p.to_be_bytes());
                v.extend_from_slice(&o.to_be_bytes());
                v
            }
            (Some(s), Some(p), None) => {
                let mut v = Vec::with_capacity(16);
                v.extend_from_slice(&s.to_be_bytes());
                v.extend_from_slice(&p.to_be_bytes());
                v
            }
            (Some(s), None, None) => s.to_be_bytes().to_vec(),
            _ => vec![],
        };

        let items = if prefix.is_empty() {
            collect_iter(&self.spog)?
        } else {
            collect_prefix(&self.spog, &prefix)?
        };

        let mut quads = Vec::new();
        for (k, _) in items {
            if k.len() < 32 {
                continue;
            }
            let q = EncodedQuad {
                subject: TermId::from_be_bytes(k[0..8].try_into().unwrap()),
                predicate: TermId::from_be_bytes(k[8..16].try_into().unwrap()),
                object: TermId::from_be_bytes(k[16..24].try_into().unwrap()),
                graph: TermId::from_be_bytes(k[24..32].try_into().unwrap()),
            };
            quads.push(q);
        }
        Ok(quads)
    }

    // ── Frontier ────────────────────────────────────────────────────────

    pub fn get_frontier(&self, graph: &GraphId) -> Result<Frontier> {
        let graph_tid = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        match self.graph_frontier.get(graph_tid.to_be_bytes())? {
            Some(v) => Ok(postcard::from_bytes(v.as_ref())?),
            None => Ok(Frontier::new()),
        }
    }

    pub fn set_frontier(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        frontier: &Frontier,
    ) -> Result<()> {
        let graph_tid = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        batch.insert(
            &self.graph_frontier,
            graph_tid.to_be_bytes(),
            &postcard::to_allocvec(frontier)?,
        );
        Ok(())
    }

    // ── Actor Counter ───────────────────────────────────────────────────

    pub fn next_counter(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        actor: &ActorId,
    ) -> Result<u64> {
        let graph_tid = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&graph_tid.to_be_bytes());
        key.extend_from_slice(actor.0.as_bytes());

        let counter = match self.actor_counter.get(&key)? {
            Some(v) => {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_be_bytes(bytes) + 1
            }
            None => 1,
        };
        batch.insert(&self.actor_counter, &key, &counter.to_be_bytes());
        Ok(counter)
    }

    // ── Batch Log ───────────────────────────────────────────────────────

    pub fn append_batch_log(
        &self,
        batch: &mut WriteBatch,
        repl_batch: &aruna_core::Batch,
    ) -> Result<()> {
        let graph_tid = self.encode_term(&EncodedTerm::from_named_node(&repl_batch.graph.0))?;
        let mut key = Vec::with_capacity(32);
        key.extend_from_slice(&graph_tid.to_be_bytes());
        key.extend_from_slice(repl_batch.actor.0.as_bytes());
        key.extend_from_slice(&repl_batch.counter.to_be_bytes());
        batch.insert(&self.batch_log, &key, &postcard::to_allocvec(repl_batch)?);
        Ok(())
    }

    pub fn batches_beyond_frontier(
        &self,
        graph: &GraphId,
        frontier: &Frontier,
    ) -> Result<Vec<aruna_core::Batch>> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_tid) = self.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let prefix = graph_tid.to_be_bytes();

        // Key layout: graph_tid(8) + actor_uuid(16) + counter(8) = 32 bytes.
        // Extract actor+counter from the key to skip deserialization for
        // batches already in the frontier.
        let mut result = Vec::new();
        for (k, v) in collect_prefix(&self.batch_log, &prefix)? {
            if k.len() < 32 {
                continue;
            }
            let actor = ActorId(uuid::Uuid::from_bytes(k[8..24].try_into().unwrap()));
            let counter = u64::from_be_bytes(k[24..32].try_into().unwrap());
            if frontier.contains(&Dot { actor, counter }) {
                continue;
            }
            let b: aruna_core::Batch = postcard::from_bytes(&v)?;
            result.push(b);
        }
        Ok(result)
    }

    // ── FTS Queue ───────────────────────────────────────────────────────

    pub fn enqueue_fts(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        subject: TermId,
    ) -> Result<()> {
        let graph_tid = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let counter = self.fts_counter.fetch_add(1, Ordering::SeqCst);
        // Fixed-width value: graph_tid (8 bytes) + subject_tid (8 bytes)
        let mut val = [0u8; 16];
        val[..8].copy_from_slice(&graph_tid.to_be_bytes());
        val[8..].copy_from_slice(&subject.to_be_bytes());
        batch.insert(&self.fts_queue, &counter.to_be_bytes(), &val);
        batch.insert(&self.fts_queue, "__next_fts", &(counter + 1).to_be_bytes());
        Ok(())
    }

    pub fn drain_fts_queue(&self, limit: usize) -> Result<Vec<(GraphId, TermId)>> {
        let mut result = Vec::new();
        let mut to_delete = Vec::new();

        for (k, v) in collect_iter(&self.fts_queue)? {
            if k.starts_with(b"__") {
                continue;
            }
            if v.len() == 16 {
                let graph_tid = TermId::from_be_bytes(v[..8].try_into().unwrap());
                let subject_tid = TermId::from_be_bytes(v[8..16].try_into().unwrap());
                let graph_term = self.decode_term(graph_tid)?;
                if let Some(nn) = graph_term.to_named_node() {
                    result.push((GraphId(nn), subject_tid));
                    to_delete.push(k);
                }
            }
            if result.len() >= limit {
                break;
            }
        }

        // Atomic batch delete (Bug 1.3 fix)
        if !to_delete.is_empty() {
            let mut batch = self.db.batch();
            for k in to_delete {
                batch.remove(&self.fts_queue, &k);
            }
            batch.commit()?;
        }
        Ok(result)
    }

    // ── Batch helpers ───────────────────────────────────────────────────

    pub fn new_batch(&self) -> WriteBatch {
        self.db.batch()
    }

    pub fn commit(&self, batch: WriteBatch) -> Result<()> {
        batch.commit()?;
        Ok(())
    }

    pub fn triples_for_subject(
        &self,
        graph: TermId,
        subject: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        let prefix = Self::subject_prefix(graph, subject);
        let mut triples = Vec::new();
        for guard in self.gspo.prefix(&prefix) {
            let (key, _) = guard.into_inner()?;
            if key.len() < 32 {
                continue;
            }
            let predicate = TermId::from_be_bytes(key[16..24].try_into().unwrap());
            let object = TermId::from_be_bytes(key[24..32].try_into().unwrap());
            triples.push((self.decode_term(predicate)?, self.decode_term(object)?));
        }
        Ok(triples)
    }

    pub fn triples_for_subject_excluding_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        excluded_predicate: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        let subject_prefix = Self::subject_prefix(graph, subject);
        let excluded_prefix = Self::subject_predicate_key(graph, subject, excluded_predicate);
        let mut triples = Vec::new();

        self.collect_triples_in_range(
            (
                Bound::Included(subject_prefix.to_vec()),
                Bound::Excluded(excluded_prefix.to_vec()),
            ),
            &mut triples,
        )?;

        if let (Some(excluded_end), Some(subject_end)) = (
            next_lexicographic_key(&excluded_prefix),
            next_lexicographic_key(&subject_prefix),
        ) {
            self.collect_triples_in_range(
                (Bound::Included(excluded_end), Bound::Excluded(subject_end)),
                &mut triples,
            )?;
        }

        Ok(triples)
    }

    pub fn count_objects_for_subject_predicate(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
    ) -> Result<usize> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = self.lookup_term(&graph_term)? else {
            return Ok(0);
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(0);
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok(0);
        };

        self.subject_predicate_count_by_ids(graph_id, subject_id, predicate_id)
    }

    pub fn objects_for_subject_predicate_page(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        offset: usize,
        limit: usize,
    ) -> Result<(usize, Vec<EncodedTerm>)> {
        if limit == 0 {
            return Ok((0, Vec::new()));
        }

        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = self.lookup_term(&graph_term)? else {
            return Ok((0, Vec::new()));
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok((0, Vec::new()));
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok((0, Vec::new()));
        };

        let prefix = Self::subject_predicate_key(graph_id, subject_id, predicate_id);
        let total = self.subject_predicate_count_by_ids(graph_id, subject_id, predicate_id)?;
        if offset >= total {
            return Ok((total, Vec::new()));
        }

        let mut objects = Vec::with_capacity(usize::min(limit, total - offset));
        for guard in self.gspo.prefix(&prefix).skip(offset).take(limit) {
            let (key, _) = guard.into_inner()?;
            if key.len() < 32 {
                continue;
            }
            let object_id = TermId::from_be_bytes(key[24..32].try_into().unwrap());
            objects.push(self.decode_term(object_id)?);
        }

        Ok((total, objects))
    }

    pub fn objects_for_subject_predicate_page_after(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        after: Option<&EncodedTerm>,
        limit: usize,
    ) -> Result<Vec<EncodedTerm>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = self.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(Vec::new());
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok(Vec::new());
        };

        let prefix = Self::subject_predicate_key(graph_id, subject_id, predicate_id);
        let Some(prefix_end) = next_lexicographic_key(&prefix) else {
            return Ok(Vec::new());
        };

        let range = if let Some(after) = after {
            let Some(after_id) = self.lookup_term(after)? else {
                return Ok(Vec::new());
            };
            let start = Self::quad_key(graph_id, subject_id, predicate_id, after_id).to_vec();
            (Bound::Excluded(start), Bound::Excluded(prefix_end))
        } else {
            (
                Bound::Included(prefix.to_vec()),
                Bound::Excluded(prefix_end),
            )
        };

        let mut objects = Vec::with_capacity(limit);
        for guard in self.gspo.range(range).take(limit) {
            let (key, _) = guard.into_inner()?;
            if key.len() < 32 {
                continue;
            }
            let object_id = TermId::from_be_bytes(key[24..32].try_into().unwrap());
            objects.push(self.decode_term(object_id)?);
        }

        Ok(objects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{BlankNode, Literal, Term};

    fn setup_store() -> (tempfile::TempDir, GraphStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        (dir, store)
    }

    fn insert_quad(
        store: &GraphStore,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
        dot: Dot,
    ) {
        if !store.contains_graph(graph).unwrap() {
            store.create_graph(graph).unwrap();
        }
        let mut batch = store.new_batch();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject_id = store.resolve_term(subject).unwrap();
        let predicate_id = store.resolve_term(predicate).unwrap();
        let object_id = store.resolve_term(object).unwrap();
        if store
            .insert_quad(
                &mut batch,
                graph_id,
                subject_id,
                predicate_id,
                object_id,
                &dot,
            )
            .unwrap()
        {
            let mut deltas = HashMap::new();
            deltas.insert((graph_id, subject_id, predicate_id), 1i64);
            store
                .apply_subject_predicate_count_deltas(&mut batch, &deltas)
                .unwrap();
        }
        store.commit(batch).unwrap();
    }

    #[test]
    fn term_dictionary_roundtrips_common_term_kinds() {
        let (_dir, store) = setup_store();
        let terms = vec![
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:named")),
            EncodedTerm::from_term(&Term::BlankNode(BlankNode::new_unchecked("b1"))),
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("hello"))),
            EncodedTerm::from_term(&Term::Literal(Literal::new_typed_literal(
                "42",
                oxrdf::NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            ))),
            EncodedTerm::from_term(&Term::Literal(
                Literal::new_language_tagged_literal_unchecked("bonjour", "fr"),
            )),
        ];

        for term in terms {
            let id = store.encode_term(&term).unwrap();
            assert_eq!(term, store.decode_term(id).unwrap());
        }
    }

    #[test]
    fn pattern_queries_match_expected_quads() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        let s1 = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:s1"));
        let s2 = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:s2"));
        let p1 = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p1"));
        let p2 = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p2"));
        let o1 = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("one")));
        let o2 = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("two")));

        insert_quad(
            &store,
            &graph,
            &s1,
            &p1,
            &o1,
            Dot {
                actor: ActorId::random(),
                counter: 1,
            },
        );
        insert_quad(
            &store,
            &graph,
            &s1,
            &p2,
            &o2,
            Dot {
                actor: ActorId::random(),
                counter: 1,
            },
        );
        insert_quad(
            &store,
            &graph,
            &s2,
            &p1,
            &o2,
            Dot {
                actor: ActorId::random(),
                counter: 1,
            },
        );

        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();
        let s1_id = store.lookup_term(&s1).unwrap().unwrap();
        let p1_id = store.lookup_term(&p1).unwrap().unwrap();
        let o2_id = store.lookup_term(&o2).unwrap().unwrap();

        assert_eq!(
            store
                .quads_for_pattern(Some(graph_id), None, None, None)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(graph_id), Some(s1_id), None, None)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(graph_id), None, Some(p1_id), None)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(graph_id), None, None, Some(o2_id))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(graph_id), Some(s1_id), Some(p1_id), None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn graph_snapshot_roundtrips_between_stores() {
        let (_dir_a, store_a) = setup_store();
        let (_dir_b, store_b) = setup_store();
        let graph = GraphId::new("urn:test:snapshot");
        let subject =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:item"));
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:label"));
        let object = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("value")));
        let actor = ActorId::random();

        insert_quad(
            &store_a,
            &graph,
            &subject,
            &predicate,
            &object,
            Dot { actor, counter: 1 },
        );
        let mut batch = store_a.new_batch();
        let mut frontier = Frontier::new();
        frontier.advance(actor, 1);
        store_a.set_frontier(&mut batch, &graph, &frontier).unwrap();
        store_a.commit(batch).unwrap();

        let snapshot = store_a.graph_snapshot(&graph).unwrap();
        store_b.import_graph_snapshot(&snapshot).unwrap();

        assert_eq!(snapshot, store_b.graph_snapshot(&graph).unwrap());
    }

    #[test]
    fn subject_predicate_counts_follow_live_quads() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:counts");
        let subject =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:item"));
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:hasPart"));
        let object_a = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:a"));
        let object_b = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:b"));
        let actor = ActorId::random();

        insert_quad(
            &store,
            &graph,
            &subject,
            &predicate,
            &object_a,
            Dot { actor, counter: 1 },
        );
        insert_quad(
            &store,
            &graph,
            &subject,
            &predicate,
            &object_b,
            Dot { actor, counter: 2 },
        );

        assert_eq!(
            store
                .count_objects_for_subject_predicate(&graph, &subject, &predicate)
                .unwrap(),
            2
        );

        let mut batch = store.new_batch();
        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();
        let subject_id = store.lookup_term(&subject).unwrap().unwrap();
        let predicate_id = store.lookup_term(&predicate).unwrap().unwrap();
        let object_a_id = store.lookup_term(&object_a).unwrap().unwrap();
        let mut frontier = Frontier::new();
        frontier.advance(actor, 2);

        assert!(
            store
                .remove_quad(
                    &mut batch,
                    graph_id,
                    subject_id,
                    predicate_id,
                    object_a_id,
                    &frontier,
                )
                .unwrap()
        );

        let mut deltas = HashMap::new();
        deltas.insert((graph_id, subject_id, predicate_id), -1i64);
        store
            .apply_subject_predicate_count_deltas(&mut batch, &deltas)
            .unwrap();
        store.commit(batch).unwrap();

        assert_eq!(
            store
                .count_objects_for_subject_predicate(&graph, &subject, &predicate)
                .unwrap(),
            1
        );
    }

    #[test]
    #[ignore = "performance smoke check"]
    fn performance_insert_and_query_smoke() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:perf");
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:label"));
        let actor = ActorId::random();
        let start = std::time::Instant::now();

        for counter in 0..2_000u64 {
            let subject = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&format!(
                "urn:test:s{counter}"
            )));
            let object = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                format!("value-{counter}"),
            )));
            insert_quad(
                &store,
                &graph,
                &subject,
                &predicate,
                &object,
                Dot {
                    actor,
                    counter: counter + 1,
                },
            );
        }

        let insert_elapsed = start.elapsed();
        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();
        let query_start = std::time::Instant::now();
        let result_count = store
            .quads_for_pattern(Some(graph_id), None, None, None)
            .unwrap()
            .len();
        let query_elapsed = query_start.elapsed();

        println!(
            "store perf: inserted {} quads in {:?}, full-graph query in {:?}",
            result_count, insert_elapsed, query_elapsed
        );
        assert_eq!(result_count, 2_000);
    }
}
