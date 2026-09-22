//! Joins visible dense index ranges with in-memory rows for graph-level operators.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;

use crate::query::context::ReadContext;
use crate::query::cursor::{DenseCursor, DenseQuad, DenseResolver, DenseTerm, RawIndexPattern};
use crate::rdf_read::{DenseScan, GraphSelector, QuadPattern, StoreReadView};
use crate::sparql::{QueryBudget, Result, SparqlError};
use crate::store::{QueryTermId, TermId};

/// Rows are fixed arrays indexed by variable, so plans are limited to this many variables.
pub(crate) const MAX_VARIABLES: usize = 8;
/// Retained key bytes per entry, used for hash budget checks.
const KEY_ENTRY_BYTES: usize = 64;
/// Dense terms stay inside one operator, so one fixed scope suffices.
const DENSE_SCOPE: u64 = 0;

/// A dense query ID with its source term, or a variable index.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Resolved {
    Constant(QueryTermId, TermId),
    Variable(usize),
}

/// Dense IDs of one partial solution, indexed by variable; unkept variables hold zero.
pub(crate) type Row = [QueryTermId; MAX_VARIABLES];

pub(crate) const EMPTY_ROW: Row = [QueryTermId(0); MAX_VARIABLES];

/// Multiply-rotate hashing for rows of store-assigned dense IDs, which requests cannot choose.
#[derive(Clone, Copy, Default)]
pub(crate) struct RowHasher(u64);

impl Hasher for RowHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.write_u64(u64::from(*byte));
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    fn write_usize(&mut self, value: usize) {
        self.write_u64(u64::try_from(value).unwrap_or(u64::MAX));
    }
}

pub(crate) type RowSet = HashSet<Row, BuildHasherDefault<RowHasher>>;
type RowGroups = HashMap<Row, Vec<usize>, BuildHasherDefault<RowHasher>>;

/// Distinct partial solutions binding the sorted `columns` variables.
#[derive(Default)]
pub(crate) struct Rows {
    pub(crate) columns: Vec<usize>,
    pub(crate) data: Vec<Row>,
}

impl Rows {
    pub(crate) fn unit() -> Self {
        Self {
            columns: Vec::new(),
            data: vec![EMPTY_ROW],
        }
    }

    pub(crate) fn value(&self, row: &Row, variable: usize) -> Option<QueryTermId> {
        self.columns.contains(&variable).then_some(row[variable])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    /// One range over all visible graphs, joined in memory with the current rows.
    Scan,
    /// One range inside the row's graph per distinct join key.
    Probe,
    /// One range per graph of the rows, joined in memory with that graph's rows.
    GraphScan,
}

/// How one pattern extends the current rows.
pub(crate) struct Step {
    pub(crate) pattern: usize,
    /// Pattern variables already bound by the rows.
    pub(crate) join: Vec<usize>,
    /// Variables kept after the step.
    pub(crate) columns: Vec<usize>,
    /// Each kept variable, and whether its value comes from the row rather than the match.
    pub(crate) kept: Vec<(usize, bool)>,
    /// The step only filters rows, so one match per join key suffices.
    pub(crate) filter: bool,
    pub(crate) method: Method,
}

/// One index range: the graph selector, bound graph, subject, predicate and object, and
/// an optional set of graphs outside which keys are skipped.
pub(crate) struct Range {
    pub(crate) selector: GraphSelector,
    pub(crate) terms: [Option<QueryTermId>; 4],
    pub(crate) graphs: Option<Rc<HashSet<QueryTermId>>>,
}

/// The pinned read view, request context and shared budget of one query.
#[derive(Clone, Copy)]
pub(crate) struct JoinInput<'a, 'view, 'context> {
    pub(crate) view: &'a StoreReadView<'view>,
    pub(crate) context: &'a ReadContext<'context>,
    pub(crate) budget: &'a QueryBudget,
}

/// Ranges opened, probes opened, and visible keys read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadCounts {
    pub(crate) scans: u64,
    pub(crate) probes: u64,
    pub(crate) scanned: u64,
}

/// Reads graph, subject, predicate and object slots of resolved patterns.
pub(crate) struct GraphReader<'a, 'view, 'context> {
    pub(crate) input: JoinInput<'a, 'view, 'context>,
    /// The graph variable, bound by the graph of every matched quad.
    graph: usize,
    pub(crate) patterns: Vec<[Resolved; 4]>,
    cache: (usize, usize),
    /// Shared by every cursor, so reverse mappings are cached once per request.
    resolver: RefCell<Option<DenseResolver>>,
    graph_sources: RefCell<HashMap<QueryTermId, TermId>>,
    counts: Cell<ReadCounts>,
}

/// An in-memory hash join of matched quads with the current rows.
struct Join<'r> {
    rows: &'r Rows,
    step: &'r Step,
    groups: RowGroups,
    output: RowSet,
}

impl<'r> Join<'r> {
    fn new(reader: &GraphReader<'_, '_, '_>, rows: &'r Rows, step: &'r Step) -> Result<Self> {
        Ok(Self {
            rows,
            step,
            groups: reader.groups(rows, &step.join)?,
            output: RowSet::default(),
        })
    }

    fn absorb(&mut self, reader: &GraphReader<'_, '_, '_>, quad: &DenseQuad) -> Result<()> {
        reader.input.budget.observe_intermediate(1)?;
        reader.count(|counts| counts.scanned += 1);
        let Some(matched) = reader.bind(self.step.pattern, quad) else {
            return Ok(());
        };
        let key = join_key(&matched, &self.step.join);
        let Self {
            rows,
            step,
            groups,
            output,
        } = self;
        if step.filter {
            // A filter step needs one match per join key, so the group is retired.
            for index in groups.remove(&key).into_iter().flatten() {
                reader.keep(output, project(&rows.data[index], &matched, step))?;
            }
        } else if let Some(members) = groups.get(&key) {
            for index in members {
                reader.keep(output, project(&rows.data[*index], &matched, step))?;
            }
        }
        Ok(())
    }

    fn finish(self) -> Rows {
        Rows {
            columns: self.step.columns.clone(),
            data: self.output.into_iter().collect(),
        }
    }
}

/// The step's kept columns, taken from the row or from the matched quad.
fn project(row: &Row, matched: &Row, step: &Step) -> Row {
    let mut output = EMPTY_ROW;
    for (column, from_row) in &step.kept {
        output[*column] = if *from_row {
            row[*column]
        } else {
            matched[*column]
        };
    }
    output
}

/// The row with every variable outside `join` cleared.
pub(crate) fn join_key(row: &Row, join: &[usize]) -> Row {
    let mut key = EMPTY_ROW;
    for variable in join {
        key[*variable] = row[*variable];
    }
    key
}

fn constant(slot: Resolved) -> Option<QueryTermId> {
    match slot {
        Resolved::Constant(term, _) => Some(term),
        Resolved::Variable(_) => None,
    }
}

impl<'a, 'view, 'context> GraphReader<'a, 'view, 'context> {
    pub(crate) fn new(input: JoinInput<'a, 'view, 'context>, graph: usize) -> Self {
        Self {
            input,
            graph,
            patterns: Vec::new(),
            cache: input.budget.dense_cache_limits(),
            resolver: RefCell::new(None),
            graph_sources: RefCell::new(HashMap::new()),
            counts: Cell::new(ReadCounts::default()),
        }
    }

    pub(crate) fn counts(&self) -> ReadCounts {
        self.counts.get()
    }

    fn count(&self, update: impl FnOnce(&mut ReadCounts)) {
        let mut counts = self.counts.get();
        update(&mut counts);
        self.counts.set(counts);
    }

    /// The source term of a graph seen in a matched quad.
    pub(crate) fn graph_source(&self, graph: QueryTermId) -> Option<TermId> {
        self.graph_sources.borrow().get(&graph).copied()
    }

    /// The source term of any dense ID read by this reader.
    pub(crate) fn source(&self, term: QueryTermId) -> Result<TermId> {
        if let Some(source) = self.graph_source(term) {
            return Ok(source);
        }
        let resolver = self.resolver.borrow();
        let resolver = resolver
            .as_ref()
            .ok_or_else(|| SparqlError::Evaluation("dense key without a resolver".to_owned()))?;
        Ok(resolver.source(DenseTerm::new(term, DENSE_SCOPE))?)
    }

    /// Opens a visible dense cursor; `None` means dense reads are unavailable.
    pub(crate) fn cursor(&self, range: Range) -> Result<Option<DenseCursor<'_, '_, '_>>> {
        let Range {
            selector,
            terms,
            graphs,
        } = range;
        let placeholder = |term: Option<QueryTermId>| term.map(|_| TermId(0));
        let (graph_hint, shape_graph) = match selector {
            GraphSelector::Named(source) => (terms[0].map(|dense| (dense, source)), Some(source)),
            GraphSelector::Union | GraphSelector::DefaultUnion => (None, None),
        };
        let scan = DenseScan {
            selector,
            shape: QuadPattern {
                graph: shape_graph,
                subject: placeholder(terms[1]),
                predicate: placeholder(terms[2]),
                object: placeholder(terms[3]),
            },
            pattern: RawIndexPattern::from_terms(terms),
            source_hints: [graph_hint, None, None, None],
            scope: DENSE_SCOPE,
            resolver: self.resolver.borrow().clone(),
            cache_entries: self.cache.0,
            cache_bytes: self.cache.1,
            graphs,
        };
        let cursor = self.input.view.dense_keys(self.input.context, scan)?;
        if let Some(cursor) = &cursor {
            let mut resolver = self.resolver.borrow_mut();
            if resolver.is_none() {
                *resolver = Some(cursor.resolver());
            }
        }
        Ok(cursor)
    }

    /// Binds one visible quad to the pattern, or `None` when repeated variables disagree.
    pub(crate) fn bind(&self, pattern: usize, quad: &DenseQuad) -> Option<Row> {
        self.graph_sources
            .borrow_mut()
            .entry(quad.graph.query())
            .or_insert(quad.graph_source);
        let mut row = EMPTY_ROW;
        let mut bound = [false; MAX_VARIABLES];
        let terms = [
            quad.graph.query(),
            quad.subject.query(),
            quad.predicate.query(),
            quad.object.query(),
        ];
        for (slot, term) in self.patterns[pattern].iter().zip(terms) {
            match *slot {
                Resolved::Constant(expected, _) if expected != term => return None,
                Resolved::Constant(..) => {}
                Resolved::Variable(variable) if bound[variable] => {
                    if row[variable] != term {
                        return None;
                    }
                }
                Resolved::Variable(variable) => {
                    row[variable] = term;
                    bound[variable] = true;
                }
            }
        }
        Some(row)
    }

    fn groups(&self, rows: &Rows, join: &[usize]) -> Result<RowGroups> {
        let mut groups = RowGroups::default();
        for (index, row) in rows.data.iter().enumerate() {
            let key = join_key(row, join);
            let entries = groups
                .len()
                .saturating_add(usize::from(!groups.contains_key(&key)));
            let bytes = entries
                .saturating_mul(KEY_ENTRY_BYTES.saturating_mul(2))
                .saturating_add(
                    index
                        .saturating_add(1)
                        .saturating_mul(std::mem::size_of::<usize>() * 2),
                );
            self.input.budget.check_hash(entries, bytes)?;
            groups.entry(key).or_default().push(index);
        }
        Ok(groups)
    }

    fn keep(&self, output: &mut RowSet, row: Row) -> Result<()> {
        if output.insert(row) {
            let entries = output.len();
            self.input
                .budget
                .check_hash(entries, entries.saturating_mul(KEY_ENTRY_BYTES))?;
        }
        Ok(())
    }

    /// Runs one step; `None` means dense reads became unavailable.
    pub(crate) fn run(&self, rows: &Rows, step: &Step) -> Result<Option<Rows>> {
        match step.method {
            Method::Scan => self.scan(rows, step),
            Method::Probe => self.probe(rows, step),
            Method::GraphScan => self.graph_scan(rows, step),
        }
    }

    /// Joins one whole-store range with the rows, skipping graphs the rows cannot match.
    fn scan(&self, rows: &Rows, step: &Step) -> Result<Option<Rows>> {
        let slots = self.patterns[step.pattern];
        let graphs = rows.columns.contains(&self.graph).then(|| {
            Rc::new(
                rows.data
                    .iter()
                    .filter_map(|row| rows.value(row, self.graph))
                    .collect::<HashSet<_>>(),
            )
        });
        let terms = [
            None,
            constant(slots[1]),
            constant(slots[2]),
            constant(slots[3]),
        ];
        let mut join = Join::new(self, rows, step)?;
        let range = Range {
            selector: GraphSelector::Union,
            terms,
            graphs,
        };
        let Some(cursor) = self.cursor(range)? else {
            return Ok(None);
        };
        self.count(|counts| counts.scans += 1);
        for quad in cursor {
            join.absorb(self, &quad?)?;
        }
        Ok(Some(join.finish()))
    }

    /// Joins one range per graph of the rows, bound by the graph and the constants only.
    fn graph_scan(&self, rows: &Rows, step: &Step) -> Result<Option<Rows>> {
        let slots = self.patterns[step.pattern];
        let mut graphs: Vec<QueryTermId> = rows
            .data
            .iter()
            .filter_map(|row| rows.value(row, self.graph))
            .collect();
        graphs.sort_unstable();
        graphs.dedup();
        let mut join = Join::new(self, rows, step)?;
        for graph in graphs {
            self.input.context.check_cancelled()?;
            let Some(source) = self.graph_source(graph) else {
                return Ok(None);
            };
            let range = Range {
                selector: GraphSelector::Named(source),
                terms: [
                    Some(graph),
                    constant(slots[1]),
                    constant(slots[2]),
                    constant(slots[3]),
                ],
                graphs: None,
            };
            let Some(cursor) = self.cursor(range)? else {
                return Ok(None);
            };
            self.count(|counts| counts.probes += 1);
            for quad in cursor {
                join.absorb(self, &quad?)?;
            }
        }
        Ok(Some(join.finish()))
    }

    /// Probes the row's graph once per distinct join key.
    fn probe(&self, rows: &Rows, step: &Step) -> Result<Option<Rows>> {
        let slots = self.patterns[step.pattern];
        let groups = self.groups(rows, &step.join)?;
        let mut output = RowSet::default();
        for (key, members) in groups {
            self.input.context.check_cancelled()?;
            let terms = slots.map(|slot| match slot {
                Resolved::Constant(term, _) => Some(term),
                Resolved::Variable(variable) => {
                    step.join.contains(&variable).then_some(key[variable])
                }
            });
            let Some(graph) = terms[0] else {
                return Ok(None);
            };
            if step.filter
                && members
                    .iter()
                    .all(|index| output.contains(&project(&rows.data[*index], &EMPTY_ROW, step)))
            {
                continue;
            }
            let Some(source) = self.graph_source(graph) else {
                return Ok(None);
            };
            let range = Range {
                selector: GraphSelector::Named(source),
                terms,
                graphs: None,
            };
            let Some(cursor) = self.cursor(range)? else {
                return Ok(None);
            };
            self.count(|counts| counts.probes += 1);
            for quad in cursor {
                let quad = quad?;
                self.input.budget.observe_intermediate(1)?;
                self.count(|counts| counts.scanned += 1);
                let Some(matched) = self.bind(step.pattern, &quad) else {
                    continue;
                };
                for index in &members {
                    self.keep(&mut output, project(&rows.data[*index], &matched, step))?;
                }
                if step.filter {
                    break;
                }
            }
        }
        Ok(Some(Rows {
            columns: step.columns.clone(),
            data: output.into_iter().collect(),
        }))
    }
}
