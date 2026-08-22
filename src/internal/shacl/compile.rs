use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use oxrdf::graph::CanonicalizationAlgorithm;
use oxrdf::{BlankNode, Graph, Literal, NamedNode, NamedOrBlankNode, Term, TermRef, Triple};
use rudof_iri::IriS;
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_core::SHACLPath;
use rudof_rdf::rdf_core::term::Object;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};
use shacl::ir::{IRComponent, IRSchema, IRShape, ShapeLabelIdx};
use shacl::rdf::ShaclParser;
use shacl::types::{NodeKind, Severity, Target};

use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::shacl::{
    CompiledShaclSchema, ShaclCompileOptions, ShaclCompileStatistics, ShaclError,
    ShaclEvaluationMode, ShaclValidationOptions, ShaclValidationReport,
};
use crate::store::{GraphStore, StoreError, TermId, hash_term};
use crate::validation_delta::{DeltaIndex, DeltaReadView};
use crate::{
    CraqleError, EncodedTerm, GraphId, GraphReplicaSnapshot, MaterializedQuadChange, Result,
    RoCrateVersion,
};

use super::dependencies;
use super::eval;
use super::model::{
    COMPILED_SHACL_FORMAT_VERSION, CompiledSchemaInner, CompiledShape, ConstraintPlan, MessagePlan,
    NodeKindPlan, PathPlan, SeverityPlan, ShapeId, ShapeKind, TargetPlan,
};
use super::resolve::{ResolvedSchema, ResolvedTarget, resolve};
use crate::cache::BoundedCache;

const CACHE_CAPACITY: usize = 32;
const CACHE_BYTES: usize = 64 * 1_048_576;
const EXTENSION_PROFILE: u32 = 0;
const RUDOF_VERSION: &str = "0.3.10";
const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const OWL_IMPORTS: &str = "<http://www.w3.org/2002/07/owl#imports>";
const SH_CONSTRAINT_COMPONENT: &str = "<http://www.w3.org/ns/shacl#ConstraintComponent>";
const SH_PROPERTY_SHAPE: &str = "<http://www.w3.org/ns/shacl#PropertyShape>";
const SH_PROPERTY: &str = "<http://www.w3.org/ns/shacl#property>";
const SH_TARGET_TYPE: &str = "<http://www.w3.org/ns/shacl#TargetType>";
const SH_PATH: &str = "<http://www.w3.org/ns/shacl#path>";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#node";
const SH_NOT: &str = "http://www.w3.org/ns/shacl#not";
const SH_QUALIFIED_VALUE_SHAPE: &str = "http://www.w3.org/ns/shacl#qualifiedValueShape";
const SH_PROPERTY_IRI: &str = "http://www.w3.org/ns/shacl#property";
const SH_AND: &str = "http://www.w3.org/ns/shacl#and";
const SH_OR: &str = "http://www.w3.org/ns/shacl#or";
const SH_XONE: &str = "http://www.w3.org/ns/shacl#xone";
const RDF_FIRST_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const SH_SPARQL: &str = "<http://www.w3.org/ns/shacl#sparql>";
const SH_RULE: &str = "<http://www.w3.org/ns/shacl#rule>";
const SH_EXPRESSION: &str = "<http://www.w3.org/ns/shacl#expression>";
const SH_JS_PREFIX: &str = "<http://www.w3.org/ns/shacl#js";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    digest: [u8; 32],
    model_version: u32,
    rudof_version: &'static str,
    rocrate_version: RoCrateVersion,
    extension_profile: u32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ValidationCacheKey {
    schema: [u8; 32],
    data_graph: String,
    data_version: [u8; 32],
    max_path_edges: u64,
    max_path_depth: usize,
    blocking_severity: crate::ShaclBlockingSeverity,
}

pub(crate) struct ShaclCompiler {
    store: Arc<GraphStore>,
    cache: Mutex<BoundedCache<CacheKey, Arc<CompiledSchemaInner>>>,
    resolved_cache: Mutex<BoundedCache<[u8; 32], Arc<ResolvedSchema>>>,
    validation_cache: Mutex<BoundedCache<ValidationCacheKey, ShaclValidationReport>>,
}

struct ValidationTimer<'a> {
    store: &'a GraphStore,
    started: Instant,
}

impl Drop for ValidationTimer<'_> {
    fn drop(&mut self) {
        self.store.record_validation(self.started.elapsed());
    }
}

impl ShaclCompiler {
    pub(crate) fn new(store: Arc<GraphStore>) -> Self {
        Self {
            store,
            cache: Mutex::new(BoundedCache::new(CACHE_CAPACITY, CACHE_BYTES)),
            resolved_cache: Mutex::new(BoundedCache::new(CACHE_CAPACITY, CACHE_BYTES)),
            validation_cache: Mutex::new(BoundedCache::new(CACHE_CAPACITY, CACHE_BYTES)),
        }
    }

    pub(crate) fn compile(
        &self,
        shapes_graph: &GraphId,
        options: &ShaclCompileOptions,
    ) -> Result<CompiledShaclSchema> {
        let materialized = materialize_shapes(&self.store, shapes_graph, options)?;
        let key = CacheKey {
            digest: materialized.digest,
            model_version: COMPILED_SHACL_FORMAT_VERSION,
            rudof_version: RUDOF_VERSION,
            rocrate_version: options.rocrate_version,
            extension_profile: EXTENSION_PROFILE,
        };
        if let Some(inner) = self.cache().get_cloned(&key) {
            return Ok(CompiledShaclSchema {
                inner,
                shape_versions: materialized.graph_versions.into(),
                statistics: ShaclCompileStatistics {
                    cache_hit: true,
                    shape_graphs: materialized.graph_count,
                    shape_triples: materialized.triple_count,
                    ..ShaclCompileStatistics::default()
                },
            });
        }

        let parse_start = Instant::now();
        let graph = OxigraphInMemory::from_str(
            &materialized.ntriples,
            &RDFFormat::NTriples,
            None,
            &ReaderMode::Strict,
        )
        .map_err(|error| ShaclError::IllFormedShapes {
            graph: shapes_graph.to_string(),
            message: error.to_string(),
        })?;
        let mut parser = ShaclParser::new(graph);
        let ast = parser
            .parse()
            .map_err(|error| ShaclError::IllFormedShapes {
                graph: shapes_graph.to_string(),
                message: error.to_string(),
            })?;
        let parse_time = parse_start.elapsed();

        let compile_start = Instant::now();
        let ir: IRSchema = ast.try_into().map_err(|error: ::shacl::error::IRError| {
            ShaclError::IllFormedShapes {
                graph: shapes_graph.to_string(),
                message: error.to_string(),
            }
        })?;
        let inner = Arc::new(compile_model(
            &ir,
            materialized.digest,
            options.rocrate_version,
        )?);
        let compile_time = compile_start.elapsed();

        self.cache()
            .insert(key, inner.clone(), materialized.ntriples.len());
        Ok(CompiledShaclSchema {
            inner,
            shape_versions: materialized.graph_versions.into(),
            statistics: ShaclCompileStatistics {
                cache_hit: false,
                shape_graphs: materialized.graph_count,
                shape_triples: materialized.triple_count,
                parse_time,
                compile_time,
            },
        })
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, BoundedCache<CacheKey, Arc<CompiledSchemaInner>>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn resolve(
        &self,
        schema: &CompiledShaclSchema,
    ) -> Result<(Arc<ResolvedSchema>, bool, std::time::Duration)> {
        let fingerprint = schema.inner.plan_fingerprint();
        let mut cache = self
            .resolved_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(resolved) = cache.get_cloned(&fingerprint) {
            return Ok((resolved, true, std::time::Duration::ZERO));
        }
        let start = Instant::now();
        let resolved = Arc::new(resolve(&self.store, schema.inner.clone())?);
        let resolve_time = start.elapsed();
        let bytes = resolved.shapes.len().saturating_mul(4_096).max(1);
        cache.insert(fingerprint, resolved.clone(), bytes);
        Ok((resolved, false, resolve_time))
    }

    pub(crate) fn versions_are_current(&self, versions: &[(GraphId, [u8; 32])]) -> Result<bool> {
        let snapshot = self.store.read_snapshot();
        for (graph, version) in versions {
            let graph = hash_term(&EncodedTerm::from_named_node(&graph.0));
            if !snapshot.contains_graph_by_id(&self.store, graph)?
                || snapshot.graph_version(&self.store, graph)? != *version
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn validate(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
        stop_after_first: bool,
    ) -> Result<ShaclValidationReport> {
        self.validate_from(
            StoreReadView::new(&self.store),
            data_graph,
            schema,
            options,
            stop_after_first,
        )
    }

    pub(crate) fn validate_authorized<F>(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
        stop_after_first: bool,
        authorize: F,
    ) -> Result<ShaclValidationReport>
    where
        F: FnOnce(&StoreReadView<'_>) -> Result<()>,
    {
        let view = StoreReadView::new(&self.store);
        authorize(&view)?;
        self.validate_from(view, data_graph, schema, options, stop_after_first)
    }

    fn validate_from(
        &self,
        view: StoreReadView<'_>,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
        stop_after_first: bool,
    ) -> Result<ShaclValidationReport> {
        #[cfg(test)]
        let _validation_probe = self.store.validation_probe();
        let _validation_timer = ValidationTimer {
            store: &self.store,
            started: Instant::now(),
        };
        if options.execution_mode == ShaclEvaluationMode::Delta {
            return Err(ShaclError::DeltaExecutionUnavailable {
                reason: "a candidate change set was not supplied".to_owned(),
            }
            .into());
        }
        self.ensure_schema_current(schema)?;
        let (resolved, cache_hit, resolve_time) = self.resolve(schema)?;
        let context = ReadContext::for_validation(options.cancellation.clone(), data_graph);
        let cache_key = self.validation_cache_key(&view, data_graph, schema, options)?;
        let estimate = match self.execution_estimate(&view, &context, data_graph, &resolved, &[]) {
            Err(CraqleError::Store(StoreError::Cancelled)) => {
                return Err(ShaclError::ValidationCancelled.into());
            }
            result => result?,
        };
        let mut report = match eval::validate_view(
            &view,
            resolved,
            data_graph,
            options,
            cache_hit,
            resolve_time,
            stop_after_first,
            Some(&context),
            None,
        ) {
            Err(CraqleError::Store(StoreError::Cancelled)) => {
                return Err(ShaclError::ValidationCancelled.into());
            }
            result => result?,
        };
        self.ensure_schema_current(schema)?;
        stamp_execution(&mut report, ShaclEvaluationMode::Full, estimate);
        if !stop_after_first {
            self.cache_validation(cache_key, report.clone());
        }
        Ok(report)
    }

    pub(crate) fn validate_delta(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        changes: &[MaterializedQuadChange],
        options: &ShaclValidationOptions,
    ) -> Result<ShaclValidationReport> {
        self.validate_delta_from(
            StoreReadView::new(&self.store),
            data_graph,
            schema,
            changes,
            options,
            None,
        )
    }

    pub(crate) fn validate_delta_authorized<F>(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        changes: &[MaterializedQuadChange],
        options: &ShaclValidationOptions,
        authorize: F,
    ) -> Result<ShaclValidationReport>
    where
        F: FnOnce(&StoreReadView<'_>) -> Result<()>,
    {
        let view = StoreReadView::new(&self.store);
        authorize(&view)?;
        self.validate_delta_from(view, data_graph, schema, changes, options, None)
    }

    pub(crate) fn validate_delta_from(
        &self,
        base: StoreReadView<'_>,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        changes: &[MaterializedQuadChange],
        options: &ShaclValidationOptions,
        base_report: Option<ShaclValidationReport>,
    ) -> Result<ShaclValidationReport> {
        #[cfg(test)]
        let _validation_probe = self.store.validation_probe();
        let _validation_timer = ValidationTimer {
            store: &self.store,
            started: Instant::now(),
        };
        if options.cancellation.is_cancelled() {
            return Err(ShaclError::ValidationCancelled.into());
        }
        self.ensure_schema_current(schema)?;
        let (resolved, cache_hit, resolve_time) = self.resolve(schema)?;
        if let Some(graph) = changed_schema(schema, changes) {
            let error = if options.execution_mode == ShaclEvaluationMode::Delta {
                ShaclError::DeltaExecutionUnavailable {
                    reason: format!(
                        "the candidate changes compiled shapes graph `{}`",
                        graph.as_str()
                    ),
                }
            } else {
                ShaclError::ShapesGraphMutationUnsupported {
                    graph: graph.to_string(),
                }
            };
            return Err(error.into());
        }
        let index = DeltaIndex::build(&self.store, data_graph, changes)?;
        let context = ReadContext::for_validation(options.cancellation.clone(), data_graph);
        let changed = match effective_changes(&base, &context, data_graph, changes) {
            Err(CraqleError::Store(StoreError::Cancelled)) => {
                return Err(ShaclError::ValidationCancelled.into());
            }
            result => result?,
        };
        let base_report = match base_report {
            Some(report) => Some(report),
            None => self
                .validation_cache()
                .get_cloned(&self.validation_cache_key(&base, data_graph, schema, options)?),
        };
        let base_available = base_report.is_some();
        let estimate =
            match self.execution_estimate(&base, &context, data_graph, &resolved, &changed) {
                Err(CraqleError::Store(StoreError::Cancelled)) => {
                    return Err(ShaclError::ValidationCancelled.into());
                }
                result => result?,
            };
        let selected = match options.execution_mode {
            ShaclEvaluationMode::Auto if auto_delta(base_available, estimate) => {
                ShaclEvaluationMode::Delta
            }
            ShaclEvaluationMode::Auto | ShaclEvaluationMode::Full => ShaclEvaluationMode::Full,
            ShaclEvaluationMode::Delta => ShaclEvaluationMode::Delta,
        };
        let mut report = match selected {
            ShaclEvaluationMode::Delta => self.validate_incremental(
                base,
                data_graph,
                schema,
                options,
                base_report,
                resolved,
                cache_hit,
                resolve_time,
                &index,
                &changed,
                &context,
            )?,
            ShaclEvaluationMode::Full => self.validate_candidate_full(
                base,
                data_graph,
                options,
                resolved,
                cache_hit,
                resolve_time,
                &index,
                &context,
            )?,
            ShaclEvaluationMode::Auto => unreachable!("auto always selects a concrete path"),
        };
        self.ensure_schema_current(schema)?;
        stamp_execution(&mut report, selected, estimate);
        Ok(report)
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_incremental(
        &self,
        base: StoreReadView<'_>,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
        base_report: Option<ShaclValidationReport>,
        resolved: Arc<ResolvedSchema>,
        cache_hit: bool,
        resolve_time: std::time::Duration,
        index: &DeltaIndex,
        changes: &[ChangedQuadIds],
        context: &ReadContext<'_>,
    ) -> Result<ShaclValidationReport> {
        let base_report = match base_report {
            Some(report) => report,
            None => {
                let base_cache_key =
                    self.validation_cache_key(&base, data_graph, schema, options)?;
                if let Some(report) = self.validation_cache().get_cloned(&base_cache_key) {
                    report
                } else {
                    let report = match eval::validate_view(
                        &base,
                        resolved.clone(),
                        data_graph,
                        options,
                        cache_hit,
                        resolve_time,
                        false,
                        Some(context),
                        None,
                    ) {
                        Err(CraqleError::Store(StoreError::Cancelled)) => {
                            return Err(ShaclError::ValidationCancelled.into());
                        }
                        Err(CraqleError::Shacl(ShaclError::ResultLimitExceeded { .. }))
                            if options.execution_mode == ShaclEvaluationMode::Delta =>
                        {
                            return Err(ShaclError::DeltaExecutionUnavailable {
                                reason: "the uncached base report exceeds the result limit"
                                    .to_owned(),
                            }
                            .into());
                        }
                        result => result?,
                    };
                    self.cache_validation(base_cache_key, report.clone());
                    report
                }
            }
        };
        let view = DeltaReadView::new(base, index);
        let graph = hash_term(&EncodedTerm::from_named_node(&data_graph.0));
        let rdf_type = hash_term(&EncodedTerm(RDF_TYPE.to_owned()));
        let selection =
            match select_incremental_targets(&view, context, graph, rdf_type, &resolved, changes) {
                Err(CraqleError::Store(StoreError::Cancelled)) => {
                    return Err(ShaclError::ValidationCancelled.into());
                }
                result => result?,
            };
        if selection.affected_pairs.is_empty() {
            let mut report = base_report;
            report.refresh_outcomes(options.blocking_severity);
            report.statistics = Default::default();
            report.statistics.shape_compile_cache_hit = cache_hit;
            report.statistics.shapes_considered = resolved.shapes.len() as u64;
            report.statistics.shapes_skipped = resolved.shapes.len() as u64;
            report.statistics.violations = report.results.len() as u64;
            report.statistics.read = context.snapshot();
            enforce_result_limit(&report, options)?;
            return Ok(report);
        }
        let mut report = match eval::validate_view(
            &view,
            resolved.clone(),
            data_graph,
            options,
            cache_hit,
            resolve_time,
            false,
            Some(context),
            Some(selection.targets),
        ) {
            Err(CraqleError::Store(StoreError::Cancelled)) => {
                return Err(ShaclError::ValidationCancelled.into());
            }
            result => result?,
        };
        report
            .results
            .extend(base_report.results.into_iter().filter(|result| {
                !selection
                    .affected_pairs
                    .contains(&(result.source_shape.clone(), result.focus_node.clone()))
            }));
        report.results.sort();
        report.refresh_outcomes(options.blocking_severity);
        report.statistics.violations = report.results.len() as u64;
        report.statistics.full_graph_fallbacks = selection.full_graph_fallbacks;
        enforce_result_limit(&report, options)?;
        Ok(report)
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_candidate_full(
        &self,
        base: StoreReadView<'_>,
        data_graph: &GraphId,
        options: &ShaclValidationOptions,
        resolved: Arc<ResolvedSchema>,
        cache_hit: bool,
        resolve_time: std::time::Duration,
        index: &DeltaIndex,
        context: &ReadContext<'_>,
    ) -> Result<ShaclValidationReport> {
        let view = DeltaReadView::new(base, index);
        match eval::validate_view(
            &view,
            resolved,
            data_graph,
            options,
            cache_hit,
            resolve_time,
            false,
            Some(context),
            None,
        ) {
            Err(CraqleError::Store(StoreError::Cancelled)) => {
                Err(ShaclError::ValidationCancelled.into())
            }
            result => result,
        }
    }

    fn execution_estimate(
        &self,
        base: &StoreReadView<'_>,
        context: &ReadContext<'_>,
        data_graph: &GraphId,
        schema: &ResolvedSchema,
        changes: &[ChangedQuadIds],
    ) -> Result<ExecutionEstimate> {
        context.check_cancelled()?;
        let graph = hash_term(&EncodedTerm::from_named_node(&data_graph.0));
        let possible_focus = changes
            .iter()
            .map(|change| change.subject)
            .collect::<HashSet<_>>()
            .len()
            .saturating_add(
                changes
                    .iter()
                    .map(|change| change.object)
                    .collect::<HashSet<_>>()
                    .len(),
            ) as u64;
        let rdf_type = hash_term(&EncodedTerm(RDF_TYPE.to_owned()));
        let mut affected = vec![false; schema.portable.shapes.len()];
        let mut global = vec![false; schema.portable.shapes.len()];
        for (index, shape) in schema.portable.shapes.iter().enumerate() {
            let dependencies = &shape.dependencies;
            let forward = dependencies
                .forward_predicates
                .iter()
                .map(hash_term)
                .collect::<HashSet<_>>();
            let inverse = dependencies
                .inverse_predicates
                .iter()
                .map(hash_term)
                .collect::<HashSet<_>>();
            for change in changes {
                let mut selected = forward.contains(&change.predicate);
                selected |= inverse.contains(&change.predicate);
                selected |= dependencies.reads_all_outgoing_predicates;
                if dependencies.reads_rdf_type && change.predicate == rdf_type {
                    selected = true;
                    global[index] |= shape.path.is_some();
                }
                affected[index] |= selected;
                if selected
                    && (dependencies.requires_global_work || dependencies.has_transitive_path)
                {
                    global[index] = true;
                }
            }
        }
        loop {
            let mut changed_any = false;
            for shape in &schema.portable.shapes {
                let parent = shape.id.0 as usize;
                for property in &shape.property_shapes {
                    let property = property.0 as usize;
                    if affected[parent] && !affected[property] {
                        affected[property] = true;
                        changed_any = true;
                    }
                    if global[parent] && !global[property] {
                        global[property] = true;
                        changed_any = true;
                    }
                }
                if !shape.dependencies.nested_shapes.is_empty()
                    && shape
                        .dependencies
                        .nested_shapes
                        .iter()
                        .any(|nested| global[nested.0 as usize] || affected[nested.0 as usize])
                {
                    if !affected[parent] {
                        affected[parent] = true;
                        changed_any = true;
                    }
                    if !global[parent] {
                        global[parent] = true;
                        changed_any = true;
                    }
                }
            }
            if !changed_any {
                break;
            }
        }

        let affected_shapes = affected.iter().filter(|affected| **affected).count() as u64;
        let Some(base_quads) = base.qv_g_count(context, graph)? else {
            return Ok(ExecutionEstimate {
                delta_work: u64::MAX,
                full_work: u64::MAX,
                affected_shapes,
                focus_nodes: possible_focus,
                full_required: true,
            });
        };
        let data_quads = candidate_count(base_quads, changes, None, None);
        let mut full_targets = vec![0u64; schema.portable.shapes.len()];
        for (index, shape) in schema.portable.shapes.iter().enumerate() {
            for target in &shape.targets {
                let Some(target_count) =
                    self.target_count(base, context, graph, target, changes, rdf_type, data_quads)?
                else {
                    return Ok(ExecutionEstimate {
                        delta_work: u64::MAX,
                        full_work: u64::MAX,
                        affected_shapes,
                        focus_nodes: possible_focus,
                        full_required: true,
                    });
                };
                full_targets[index] = full_targets[index].saturating_add(target_count);
            }
        }
        loop {
            let mut changed_any = false;
            for shape in &schema.portable.shapes {
                let parent = shape.id.0 as usize;
                for property in &shape.property_shapes {
                    let property = property.0 as usize;
                    let next = full_targets[property].max(full_targets[parent]);
                    changed_any |= next != full_targets[property];
                    full_targets[property] = next;
                }
            }
            if !changed_any {
                break;
            }
        }
        let broad_targets = schema
            .portable
            .shapes
            .iter()
            .flat_map(|shape| shape.targets.iter())
            .filter(|target| !matches!(target, TargetPlan::Node(_)))
            .count();
        let broad_targets = u64::try_from(broad_targets).unwrap_or(u64::MAX);
        let scan_work = base_quads
            .checked_mul(broad_targets)
            .and_then(|work| work.checked_add(SCAN_DIV.saturating_sub(1)))
            .map_or(u64::MAX, |work| work / SCAN_DIV);
        let full_work = schema
            .portable
            .shapes
            .iter()
            .enumerate()
            .filter(|(_, shape)| !shape.deactivated)
            .fold(
                (schema.portable.shapes.len() as u64).saturating_mul(FULL_SHAPE_WORK),
                |work, (index, shape)| {
                    work.saturating_add(full_targets[index].saturating_mul(shape_work(shape)))
                },
            )
            .saturating_add(scan_work)
            .saturating_add((schema.portable.shapes.len() as u64).saturating_mul(FULL_SHAPE_WORK));
        let mut delta_work = (changes.len() as u64).saturating_mul(2).max(1);
        for (index, shape) in schema.portable.shapes.iter().enumerate() {
            if affected[index] && !shape.deactivated {
                let dependency_work = shape.dependencies.forward_predicates.len()
                    + shape.dependencies.inverse_predicates.len()
                    + usize::from(shape.dependencies.reads_rdf_type);
                let focus_work = 1u64
                    .saturating_add(shape.constraints.len() as u64)
                    .saturating_add(dependency_work as u64);
                delta_work =
                    delta_work.saturating_add(possible_focus.max(1).saturating_mul(focus_work));
            }
        }
        let full_required = global.iter().any(|global| *global);
        if full_required {
            delta_work = delta_work.max(full_work.saturating_add(1));
        }
        Ok(ExecutionEstimate {
            delta_work,
            full_work,
            affected_shapes,
            focus_nodes: possible_focus,
            full_required,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn target_count(
        &self,
        base: &StoreReadView<'_>,
        context: &ReadContext<'_>,
        graph: TermId,
        target: &TargetPlan,
        changes: &[ChangedQuadIds],
        rdf_type: TermId,
        cap: u64,
    ) -> Result<Option<u64>> {
        let (predicate, object) = match target {
            TargetPlan::Node(_) => return Ok(Some(1)),
            TargetPlan::Class(class) | TargetPlan::ImplicitClass(class) => {
                (Some(rdf_type), Some(hash_term(class)))
            }
            TargetPlan::SubjectsOf(predicate) | TargetPlan::ObjectsOf(predicate) => {
                (Some(hash_term(predicate)), None)
            }
        };
        let count = match object {
            Some(object) => base.qv_gpo_count(context, graph, predicate.unwrap(), object)?,
            None => base.qv_gp_count(context, graph, predicate.unwrap())?,
        };
        Ok(count.map(|count| candidate_count(count, changes, predicate, object).min(cap)))
    }

    fn ensure_schema_current(&self, schema: &CompiledShaclSchema) -> Result<()> {
        if self.versions_are_current(schema.shape_versions())? {
            return Ok(());
        }
        Err(ShaclError::SchemaChangedDuringValidation {
            graph: schema
                .shape_versions()
                .first()
                .map(|(graph, _)| graph.to_string())
                .unwrap_or_else(|| "compiled schema".to_owned()),
        }
        .into())
    }

    fn validation_cache_key(
        &self,
        view: &StoreReadView<'_>,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
    ) -> Result<ValidationCacheKey> {
        let graph = crate::store::hash_term(&EncodedTerm::from_named_node(&data_graph.0));
        Ok(ValidationCacheKey {
            schema: schema.inner.plan_fingerprint(),
            data_graph: data_graph.to_string(),
            data_version: view.snapshot().graph_version(&self.store, graph)?,
            max_path_edges: options.max_path_edges,
            max_path_depth: options.max_path_depth,
            blocking_severity: options.blocking_severity,
        })
    }

    fn validation_cache(
        &self,
    ) -> std::sync::MutexGuard<'_, BoundedCache<ValidationCacheKey, ShaclValidationReport>> {
        self.validation_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cache_validation(&self, key: ValidationCacheKey, report: ShaclValidationReport) {
        let bytes = postcard::to_allocvec(&report)
            .map(|encoded| encoded.len())
            .unwrap_or(CACHE_BYTES);
        let mut cache = self.validation_cache();
        cache.insert(key, report, bytes);
    }

    pub(crate) fn seed_validation_report(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        data_version: [u8; 32],
        options: &ShaclValidationOptions,
        report: ShaclValidationReport,
    ) {
        self.cache_validation(
            ValidationCacheKey {
                schema: schema.inner.plan_fingerprint(),
                data_graph: data_graph.to_string(),
                data_version,
                max_path_edges: options.max_path_edges,
                max_path_depth: options.max_path_depth,
                blocking_severity: options.blocking_severity,
            },
            report,
        );
    }

    pub(crate) fn cache_current_report(
        &self,
        data_graph: &GraphId,
        schema: &CompiledShaclSchema,
        options: &ShaclValidationOptions,
        report: ShaclValidationReport,
    ) -> Result<()> {
        let view = StoreReadView::new(&self.store);
        let key = self.validation_cache_key(&view, data_graph, schema, options)?;
        self.cache_validation(key, report);
        Ok(())
    }
}

struct IncrementalSelection {
    targets: Vec<BTreeSet<TermId>>,
    affected_pairs: HashSet<(EncodedTerm, EncodedTerm)>,
    full_graph_fallbacks: u64,
}

#[derive(Clone, Copy)]
struct ChangedQuadIds {
    subject: TermId,
    predicate: TermId,
    object: TermId,
    present: bool,
}

#[derive(Clone, Copy)]
struct ExecutionEstimate {
    delta_work: u64,
    full_work: u64,
    affected_shapes: u64,
    focus_nodes: u64,
    full_required: bool,
}

fn auto_delta(base_available: bool, estimate: ExecutionEstimate) -> bool {
    base_available
        && !estimate.full_required
        && estimate.delta_work < u64::MAX
        && estimate.full_work < u64::MAX
        && estimate.delta_work <= estimate.full_work
}

fn stamp_execution(
    report: &mut ShaclValidationReport,
    selected: ShaclEvaluationMode,
    estimate: ExecutionEstimate,
) {
    report.statistics.selected_mode = selected;
    report.statistics.estimated_delta_work = estimate.delta_work;
    report.statistics.estimated_full_work = estimate.full_work;
    report.statistics.estimated_affected_shapes = estimate.affected_shapes;
    report.statistics.estimated_focus_nodes = estimate.focus_nodes;
}

fn change_graph(change: &MaterializedQuadChange) -> &GraphId {
    match change {
        MaterializedQuadChange::Insert { graph, .. }
        | MaterializedQuadChange::Delete { graph, .. } => graph,
    }
}

fn changed_schema<'a>(
    schema: &CompiledShaclSchema,
    changes: &'a [MaterializedQuadChange],
) -> Option<&'a GraphId> {
    changes.iter().map(change_graph).find(|graph| {
        schema
            .shape_versions()
            .iter()
            .any(|(dependency, _)| dependency == *graph)
    })
}

fn changed_quad_ids(
    data_graph: &GraphId,
    changes: &[MaterializedQuadChange],
) -> Vec<ChangedQuadIds> {
    let mut states = BTreeMap::new();
    for change in changes {
        let (change_graph, subject, predicate, object, present) = match change {
            MaterializedQuadChange::Insert {
                graph,
                subject,
                predicate,
                object,
            } => (graph, subject, predicate, object, true),
            MaterializedQuadChange::Delete {
                graph,
                subject,
                predicate,
                object,
            } => (graph, subject, predicate, object, false),
        };
        if change_graph == data_graph {
            let state = ChangedQuadIds {
                subject: hash_term(subject),
                predicate: hash_term(predicate),
                object: hash_term(object),
                present,
            };
            states.insert((state.subject, state.predicate, state.object), state);
        }
    }
    states.into_values().collect()
}

fn effective_changes(
    base: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    data_graph: &GraphId,
    changes: &[MaterializedQuadChange],
) -> Result<Vec<ChangedQuadIds>> {
    let graph = hash_term(&EncodedTerm::from_named_node(&data_graph.0));
    let mut effective = Vec::new();
    for change in changed_quad_ids(data_graph, changes) {
        let base_present = base.exists(
            context,
            GraphSelector::Named(graph),
            QuadPattern {
                subject: Some(change.subject),
                predicate: Some(change.predicate),
                object: Some(change.object),
                ..QuadPattern::default()
            },
        )?;
        if base_present != change.present {
            effective.push(change);
        }
    }
    Ok(effective)
}

fn candidate_count(
    base: u64,
    changes: &[ChangedQuadIds],
    predicate: Option<TermId>,
    object: Option<TermId>,
) -> u64 {
    let delta = changes.iter().fold(0i64, |delta, change| {
        if predicate.is_some_and(|expected| change.predicate != expected)
            || object.is_some_and(|expected| change.object != expected)
        {
            delta
        } else if change.present {
            delta.saturating_add(1)
        } else {
            delta.saturating_sub(1)
        }
    });
    if delta.is_negative() {
        base.saturating_sub(delta.unsigned_abs())
    } else {
        base.saturating_add(delta as u64)
    }
}

fn shape_work(shape: &CompiledShape) -> u64 {
    1u64.saturating_add(shape.constraints.len() as u64)
        .saturating_add(u64::from(shape.path.is_some()))
        .saturating_add(shape.dependencies.nested_shapes.len() as u64)
}

const FULL_SHAPE_WORK: u64 = 20;
const SCAN_DIV: u64 = 20;

#[allow(clippy::too_many_arguments)]
fn select_incremental_targets<V: RdfReadView>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    rdf_type: TermId,
    schema: &ResolvedSchema,
    changes: &[ChangedQuadIds],
) -> Result<IncrementalSelection> {
    let shape_count = schema.shapes.len();
    let mut candidates = vec![BTreeSet::new(); shape_count];
    let mut global = vec![false; shape_count];

    for (index, shape) in schema.portable.shapes.iter().enumerate() {
        let dependencies = &shape.dependencies;
        let forward = dependencies
            .forward_predicates
            .iter()
            .map(hash_term)
            .collect::<HashSet<_>>();
        let inverse = dependencies
            .inverse_predicates
            .iter()
            .map(hash_term)
            .collect::<HashSet<_>>();
        for change in changes {
            let mut affected = false;
            if forward.contains(&change.predicate) {
                candidates[index].insert(change.subject);
                affected = true;
            }
            if inverse.contains(&change.predicate) {
                candidates[index].insert(change.object);
                affected = true;
            }
            if dependencies.reads_all_outgoing_predicates {
                candidates[index].insert(change.subject);
                affected = true;
            }
            if dependencies.reads_rdf_type && change.predicate == rdf_type {
                candidates[index].insert(change.subject);
                affected = true;
                if shape.path.is_some() {
                    global[index] = true;
                }
            }
            if affected && (dependencies.requires_global_work || dependencies.has_transitive_path) {
                global[index] = true;
            }
        }
    }

    loop {
        let mut changed_any = false;
        for shape in &schema.portable.shapes {
            let parent = shape.id.0 as usize;
            for property in &shape.property_shapes {
                let property = property.0 as usize;
                let inherited = candidates[parent].clone();
                let before = candidates[property].len();
                candidates[property].extend(inherited);
                changed_any |= candidates[property].len() != before;
                if global[parent] && !global[property] {
                    global[property] = true;
                    changed_any = true;
                }
            }
            if !shape.dependencies.nested_shapes.is_empty()
                && shape.dependencies.nested_shapes.iter().any(|nested| {
                    global[nested.0 as usize] || !candidates[nested.0 as usize].is_empty()
                })
                && !global[parent]
            {
                global[parent] = true;
                changed_any = true;
            }
        }
        if !changed_any {
            break;
        }
    }

    let parents = property_shape_parents(schema);
    let mut targets = vec![BTreeSet::new(); shape_count];
    let mut full_graph_fallbacks = 0u64;
    if global.iter().any(|global| *global) {
        let mut target_work = super::targets::TargetWork::default();
        let all_targets = super::targets::resolve_targets(
            view,
            context,
            graph,
            rdf_type,
            schema,
            &mut target_work,
        )?;
        for index in 0..shape_count {
            if global[index] {
                targets[index] = all_targets[index].clone();
                full_graph_fallbacks = full_graph_fallbacks.saturating_add(1);
            }
        }
    }
    for index in 0..shape_count {
        if global[index] {
            continue;
        }
        for focus in candidates[index].iter().copied() {
            if focus_is_target(
                view,
                context,
                graph,
                rdf_type,
                schema,
                &parents,
                index,
                focus,
                &mut HashSet::new(),
            )? {
                targets[index].insert(focus);
            }
        }
    }

    let mut affected_pairs = HashSet::new();
    for index in 0..shape_count {
        let mut affected_focus = candidates[index].clone();
        if global[index] {
            affected_focus.extend(targets[index].iter().copied());
        }
        for focus in affected_focus {
            affected_pairs.insert((
                schema.portable.shapes[index].label.clone(),
                view.decode_term(context, focus)?,
            ));
        }
    }
    Ok(IncrementalSelection {
        targets,
        affected_pairs,
        full_graph_fallbacks,
    })
}

fn property_shape_parents(schema: &ResolvedSchema) -> Vec<Vec<usize>> {
    let mut parents = vec![Vec::new(); schema.shapes.len()];
    for shape in &schema.portable.shapes {
        for property in &shape.property_shapes {
            parents[property.0 as usize].push(shape.id.0 as usize);
        }
    }
    parents
}

#[allow(clippy::too_many_arguments)]
fn focus_is_target<V: RdfReadView>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    rdf_type: TermId,
    schema: &ResolvedSchema,
    parents: &[Vec<usize>],
    shape: usize,
    focus: TermId,
    visiting: &mut HashSet<usize>,
) -> Result<bool> {
    if !visiting.insert(shape) {
        return Ok(false);
    }
    for target in &schema.shapes[shape].targets {
        let selected = match target {
            ResolvedTarget::Node(node) => *node == focus,
            ResolvedTarget::Class(class) | ResolvedTarget::ImplicitClass(class) => view.exists(
                context,
                GraphSelector::Named(graph),
                QuadPattern {
                    subject: Some(focus),
                    predicate: Some(rdf_type),
                    object: Some(*class),
                    ..QuadPattern::default()
                },
            )?,
            ResolvedTarget::SubjectsOf(predicate) => view.exists(
                context,
                GraphSelector::Named(graph),
                QuadPattern {
                    subject: Some(focus),
                    predicate: Some(*predicate),
                    ..QuadPattern::default()
                },
            )?,
            ResolvedTarget::ObjectsOf(predicate) => view.exists(
                context,
                GraphSelector::Named(graph),
                QuadPattern {
                    predicate: Some(*predicate),
                    object: Some(focus),
                    ..QuadPattern::default()
                },
            )?,
        };
        if selected {
            visiting.remove(&shape);
            return Ok(true);
        }
    }
    for parent in &parents[shape] {
        if focus_is_target(
            view, context, graph, rdf_type, schema, parents, *parent, focus, visiting,
        )? {
            visiting.remove(&shape);
            return Ok(true);
        }
    }
    visiting.remove(&shape);
    Ok(false)
}

fn enforce_result_limit(
    report: &ShaclValidationReport,
    options: &ShaclValidationOptions,
) -> Result<()> {
    if report.results.len() > options.max_results {
        return Err(ShaclError::ResultLimitExceeded {
            limit: options.max_results,
        }
        .into());
    }
    Ok(())
}

struct MaterializedShapes {
    digest: [u8; 32],
    ntriples: String,
    graph_count: usize,
    triple_count: usize,
    graph_versions: Vec<(GraphId, [u8; 32])>,
}

fn materialize_shapes(
    store: &GraphStore,
    root: &GraphId,
    options: &ShaclCompileOptions,
) -> Result<MaterializedShapes> {
    let mut graphs = BTreeMap::new();
    let mut stack = Vec::new();
    visit_shape_graph(store, root, options, &mut graphs, &mut stack)?;

    let mut graph_versions = Vec::with_capacity(graphs.len());
    for snapshot in graphs.values() {
        let clock = postcard::to_allocvec(&snapshot.clock).map_err(StoreError::from)?;
        graph_versions.push((snapshot.graph.clone(), *blake3::hash(&clock).as_bytes()));
    }

    let mut graph_union = Graph::new();
    let mut property_shapes = BTreeSet::new();
    let mut path_counts = BTreeMap::new();
    for (graph, snapshot) in &graphs {
        let scope_hash = blake3::hash(graph.as_bytes()).to_hex();
        let scope = &scope_hash.as_str()[..16];
        for quad in &snapshot.quads {
            let encoded_subject = scoped_term(&quad.subject, scope);
            let encoded_object = scoped_term(&quad.object, scope);
            if quad.predicate.0 == RDF_TYPE && quad.object.0 == SH_PROPERTY_SHAPE {
                property_shapes.insert(encoded_subject.clone());
            } else if quad.predicate.0 == SH_PROPERTY {
                property_shapes.insert(encoded_object.clone());
            }
            if quad.predicate.0 == SH_PATH {
                *path_counts.entry(encoded_subject.clone()).or_insert(0usize) += 1;
            }

            let subject = encoded_subject
                .to_term()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.subject))?;
            let subject = NamedOrBlankNode::try_from(subject)
                .map_err(|_| ill_formed_term(snapshot, &quad.subject))?;
            let predicate = quad
                .predicate
                .to_named_node()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.predicate))?;
            let object = encoded_object
                .to_term()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.object))?;
            graph_union.insert(&Triple::new(subject, predicate, object));
        }
    }
    if let Some(shape) = property_shapes
        .iter()
        .find(|shape| path_counts.get(*shape).copied() != Some(1))
    {
        return Err(ShaclError::IllFormedShapes {
            graph: root.to_string(),
            message: format!("property shape {} must have exactly one sh:path", shape.0),
        }
        .into());
    }
    reject_recursive_shapes(&graph_union)?;
    graph_union.canonicalize(CanonicalizationAlgorithm::Unstable);
    let mut triples = graph_union
        .iter()
        .map(|triple| triple.to_string())
        .collect::<Vec<_>>();
    triples.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"craqle-shacl-schema/v1\0");
    let mut ntriples = String::new();
    for triple in &triples {
        hash_field(&mut hasher, triple.as_bytes());
        ntriples.push_str(triple);
        ntriples.push_str(" .\n");
    }
    Ok(MaterializedShapes {
        digest: *hasher.finalize().as_bytes(),
        ntriples,
        graph_count: graphs.len(),
        triple_count: triples.len(),
        graph_versions,
    })
}

fn reject_recursive_shapes(graph: &Graph) -> Result<()> {
    let mut edges = BTreeMap::<String, BTreeSet<String>>::new();
    let mut list_first = HashMap::<String, String>::new();
    let mut list_rest = HashMap::<String, String>::new();
    let mut list_references = Vec::new();

    for triple in graph.iter() {
        let subject = triple.subject.to_string();
        let object = match triple.object {
            TermRef::NamedNode(node) => node.to_string(),
            TermRef::BlankNode(node) => node.to_string(),
            TermRef::Literal(_) => continue,
            TermRef::Triple(value) => {
                return Err(ShaclError::UnsupportedRdfStarTerm {
                    term: value.to_string(),
                }
                .into());
            }
        };
        match triple.predicate.as_str() {
            SH_NODE | SH_NOT | SH_QUALIFIED_VALUE_SHAPE | SH_PROPERTY_IRI => {
                edges.entry(subject).or_default().insert(object);
            }
            SH_AND | SH_OR | SH_XONE => list_references.push((subject, object)),
            RDF_FIRST_IRI => {
                list_first.insert(subject, object);
            }
            RDF_REST_IRI => {
                list_rest.insert(subject, object);
            }
            _ => {}
        }
    }

    for (shape, mut list) in list_references {
        let mut visited = HashSet::new();
        while list != format!("<{RDF_NIL_IRI}>") && visited.insert(list.clone()) {
            let Some(referenced) = list_first.get(&list) else {
                break;
            };
            edges
                .entry(shape.clone())
                .or_default()
                .insert(referenced.clone());
            let Some(next) = list_rest.get(&list) else {
                break;
            };
            list.clone_from(next);
        }
    }

    let mut complete = HashSet::new();
    let mut active = HashSet::new();
    for shape in edges.keys() {
        if let Some(shape) = recursive_shape(shape, &edges, &mut active, &mut complete) {
            return Err(ShaclError::UnsupportedRecursiveShape { shape }.into());
        }
    }
    Ok(())
}

fn recursive_shape(
    shape: &str,
    edges: &BTreeMap<String, BTreeSet<String>>,
    active: &mut HashSet<String>,
    complete: &mut HashSet<String>,
) -> Option<String> {
    if active.contains(shape) {
        return Some(shape.to_owned());
    }
    if complete.contains(shape) {
        return None;
    }
    active.insert(shape.to_owned());
    for nested in edges.get(shape).into_iter().flatten() {
        if let Some(recursive) = recursive_shape(nested, edges, active, complete) {
            return Some(recursive);
        }
    }
    active.remove(shape);
    complete.insert(shape.to_owned());
    None
}

fn ill_formed_term(snapshot: &GraphReplicaSnapshot, term: &EncodedTerm) -> ShaclError {
    ShaclError::IllFormedShapes {
        graph: snapshot.graph.to_string(),
        message: format!("invalid RDF term {}", term.0),
    }
}

fn visit_shape_graph(
    store: &GraphStore,
    graph: &GraphId,
    options: &ShaclCompileOptions,
    graphs: &mut BTreeMap<String, GraphReplicaSnapshot>,
    stack: &mut Vec<String>,
) -> Result<()> {
    if let Some(cycle_start) = stack
        .iter()
        .position(|candidate| candidate == graph.as_str())
    {
        let mut cycle = stack[cycle_start..].to_vec();
        cycle.push(graph.to_string());
        return Err(ShaclError::ImportCycle { graphs: cycle }.into());
    }
    if graphs.contains_key(graph.as_str()) {
        return Ok(());
    }
    if !store.contains_graph(graph)? {
        return Err(ShaclError::ShapesGraphNotFound {
            graph: graph.to_string(),
        }
        .into());
    }

    stack.push(graph.to_string());
    let snapshot = store.graph_snapshot(graph)?;
    reject_unsupported_raw(&snapshot)?;
    let imports = imports(store, graph)?;
    for import in imports {
        if !options.allow_local_imports {
            return Err(ShaclError::ImportsDisabled {
                graph: graph.to_string(),
                import,
            }
            .into());
        }
        let imported = GraphId::new(&import);
        if !store.contains_graph(&imported)? {
            return Err(ShaclError::ImportNotLocal {
                graph: graph.to_string(),
                import,
            }
            .into());
        }
        visit_shape_graph(store, &imported, options, graphs, stack)?;
    }
    stack.pop();
    graphs.insert(graph.to_string(), snapshot);
    Ok(())
}

fn imports(store: &GraphStore, graph: &GraphId) -> Result<Vec<String>> {
    let view = StoreReadView::new(store);
    let context = ReadContext::for_validation(crate::QueryCancellation::new(), graph);
    let graph_id = hash_term(&EncodedTerm::from_named_node(&graph.0));
    let imports_id = hash_term(&EncodedTerm(OWL_IMPORTS.to_owned()));
    let mut imports = Vec::new();
    for quad in view.scan(
        &context,
        GraphSelector::Named(graph_id),
        QuadPattern {
            predicate: Some(imports_id),
            ..QuadPattern::default()
        },
    )? {
        let object = view.decode_term(&context, quad?.object)?;
        let Some(import) = named_iri(&object) else {
            return Err(ShaclError::IllFormedShapes {
                graph: graph.to_string(),
                message: format!("owl:imports object must be an IRI, got {}", object.0),
            }
            .into());
        };
        imports.push(import.to_owned());
    }
    imports.sort();
    imports.dedup();
    Ok(imports)
}

fn reject_unsupported_raw(snapshot: &GraphReplicaSnapshot) -> Result<()> {
    for quad in &snapshot.quads {
        if quad.subject.0.starts_with("<<") || quad.object.0.starts_with("<<") {
            let term = if quad.subject.0.starts_with("<<") {
                &quad.subject
            } else {
                &quad.object
            };
            return Err(ShaclError::UnsupportedRdfStarTerm {
                term: term.0.clone(),
            }
            .into());
        }
        let component = if quad.predicate.0 == SH_SPARQL {
            Some("http://www.w3.org/ns/shacl#SPARQLConstraintComponent")
        } else if quad.predicate.0 == SH_RULE {
            Some("http://www.w3.org/ns/shacl#rule")
        } else if quad.predicate.0 == SH_EXPRESSION {
            Some("http://www.w3.org/ns/shacl#expression")
        } else if quad.predicate.0.starts_with(SH_JS_PREFIX) {
            Some("http://www.w3.org/ns/shacl#JSConstraint")
        } else if quad.predicate.0 == RDF_TYPE
            && (quad.object.0 == SH_CONSTRAINT_COMPONENT || quad.object.0 == SH_TARGET_TYPE)
        {
            named_iri(&quad.object)
        } else {
            None
        };
        if let Some(component) = component {
            return Err(ShaclError::UnsupportedComponent {
                shape: quad.subject.0.clone(),
                component: component.to_owned(),
            }
            .into());
        }
    }
    Ok(())
}

fn named_iri(term: &EncodedTerm) -> Option<&str> {
    term.0.strip_prefix('<')?.strip_suffix('>')
}

fn scoped_term(term: &EncodedTerm, scope: &str) -> EncodedTerm {
    match term.0.strip_prefix("_:") {
        Some(label) => EncodedTerm(format!("_:g{scope}_{label}")),
        None => term.clone(),
    }
}

fn hash_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn compile_model(
    schema: &IRSchema,
    schema_hash: [u8; 32],
    rocrate_version: RoCrateVersion,
) -> Result<CompiledSchemaInner> {
    let mut entries = Vec::new();
    for (_, shape) in schema.iter() {
        entries.push((encoded_object(shape.id())?, shape));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let labels: HashMap<_, _> = entries
        .iter()
        .enumerate()
        .map(|(index, (label, _))| (label.clone(), ShapeId(index as u32)))
        .collect();

    let mut shapes = Vec::with_capacity(entries.len());
    for (label, shape) in entries {
        let id = *labels.get(&label).expect("shape label indexed above");
        let targets = shape
            .targets()
            .iter()
            .map(|target| compile_target(&label, target))
            .collect::<Result<Vec<_>>>()?;
        let path = shape.path().map(compile_path).transpose()?;
        let property_shapes = shape
            .property_shapes()
            .iter()
            .map(|index| shape_id(schema, &labels, index))
            .collect::<Result<Vec<_>>>()?;
        let mut constraints = Vec::new();
        for component in shape.components() {
            if let Some(component) = compile_component(schema, &labels, &label, component)? {
                constraints.push(component);
            }
        }
        if shape.reifier_info().is_some() {
            return Err(ShaclError::UnsupportedComponent {
                shape: label.0.clone(),
                component: "http://www.w3.org/ns/shacl#reifierShape".to_owned(),
            }
            .into());
        }
        let messages = compile_messages(shape);
        let dependencies =
            dependencies::analyze(&targets, path.as_ref(), &constraints, &property_shapes);
        shapes.push(CompiledShape {
            id,
            label,
            kind: match shape {
                IRShape::NodeShape(_) => ShapeKind::Node,
                IRShape::PropertyShape(_) => ShapeKind::Property,
            },
            targets: targets.into_boxed_slice(),
            path,
            constraints: constraints.into_boxed_slice(),
            property_shapes: property_shapes.into_boxed_slice(),
            severity: compile_severity(shape.severity()),
            messages,
            deactivated: shape.deactivated(),
            dependencies,
        });
    }
    Ok(CompiledSchemaInner {
        format_version: COMPILED_SHACL_FORMAT_VERSION,
        schema_hash,
        rocrate_version,
        shapes: shapes.into_boxed_slice(),
    })
}

fn compile_target(shape: &EncodedTerm, target: &Target) -> Result<TargetPlan> {
    match target {
        Target::Node(node) => Ok(TargetPlan::Node(encoded_object(node)?)),
        Target::Class(class) => Ok(TargetPlan::Class(encoded_object(class)?)),
        Target::SubjectsOf(predicate) => Ok(TargetPlan::SubjectsOf(encoded_iri(predicate))),
        Target::ObjectsOf(predicate) => Ok(TargetPlan::ObjectsOf(encoded_iri(predicate))),
        Target::ImplicitClass(class) => Ok(TargetPlan::ImplicitClass(encoded_object(class)?)),
        Target::WrongNode(_)
        | Target::WrongClass(_)
        | Target::WrongSubjectsOf(_)
        | Target::WrongObjectsOf(_)
        | Target::WrongImplicitClass(_) => Err(ShaclError::IllFormedShapes {
            graph: shape.0.clone(),
            message: format!("ill-formed target declaration: {target}"),
        }
        .into()),
    }
}

fn compile_path(path: &SHACLPath) -> Result<PathPlan> {
    Ok(match path {
        SHACLPath::Predicate { pred } => PathPlan::Predicate(encoded_iri(pred)),
        SHACLPath::Alternative { paths } => PathPlan::Alternative(
            paths
                .iter()
                .map(compile_path)
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
        ),
        SHACLPath::Sequence { paths } => PathPlan::Sequence(
            paths
                .iter()
                .map(compile_path)
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
        ),
        SHACLPath::Inverse { path } => PathPlan::Inverse(Box::new(compile_path(path)?)),
        SHACLPath::ZeroOrMore { path } => PathPlan::ZeroOrMore(Box::new(compile_path(path)?)),
        SHACLPath::OneOrMore { path } => PathPlan::OneOrMore(Box::new(compile_path(path)?)),
        SHACLPath::ZeroOrOne { path } => PathPlan::ZeroOrOne(Box::new(compile_path(path)?)),
    })
}

fn compile_component(
    schema: &IRSchema,
    labels: &HashMap<EncodedTerm, ShapeId>,
    shape: &EncodedTerm,
    component: &IRComponent,
) -> Result<Option<ConstraintPlan>> {
    let plan = match component {
        IRComponent::Class(value) => ConstraintPlan::Class(encoded_object(value.class_rule())?),
        IRComponent::Datatype(value) => ConstraintPlan::Datatype(encoded_iri(value.datatype())),
        IRComponent::NodeKind(value) => ConstraintPlan::NodeKind(match value.node_kind() {
            NodeKind::Iri => NodeKindPlan::Iri,
            NodeKind::Lit => NodeKindPlan::Literal,
            NodeKind::BNode => NodeKindPlan::BlankNode,
            NodeKind::BNodeOrIri => NodeKindPlan::BlankNodeOrIri,
            NodeKind::BNodeOrLit => NodeKindPlan::BlankNodeOrLiteral,
            NodeKind::IriOrLit => NodeKindPlan::IriOrLiteral,
        }),
        IRComponent::MinCount(value) => ConstraintPlan::MinCount(value.min_count()),
        IRComponent::MaxCount(value) => ConstraintPlan::MaxCount(value.max_count()),
        IRComponent::MinExclusive(value) => {
            ConstraintPlan::MinExclusive(encoded_literal(value.min_exclusive())?)
        }
        IRComponent::MaxExclusive(value) => {
            ConstraintPlan::MaxExclusive(encoded_literal(value.max_exclusive())?)
        }
        IRComponent::MinInclusive(value) => {
            ConstraintPlan::MinInclusive(encoded_literal(value.min_inclusive())?)
        }
        IRComponent::MaxInclusive(value) => {
            ConstraintPlan::MaxInclusive(encoded_literal(value.max_inclusive())?)
        }
        IRComponent::MinLength(value) => {
            ConstraintPlan::MinLength(nonnegative(shape, "sh:minLength", value.min_length())?)
        }
        IRComponent::MaxLength(value) => {
            ConstraintPlan::MaxLength(nonnegative(shape, "sh:maxLength", value.max_length())?)
        }
        IRComponent::Pattern(value) => ConstraintPlan::Pattern {
            pattern: value.pattern().clone(),
            flags: value.flags().cloned(),
        },
        IRComponent::UniqueLang(value) => ConstraintPlan::UniqueLang(value.unique_lang()),
        IRComponent::LanguageIn(value) => ConstraintPlan::LanguageIn(
            value
                .langs()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
        IRComponent::Equals(value) => ConstraintPlan::Equals(encoded_iri(value.iri())),
        IRComponent::Disjoint(value) => ConstraintPlan::Disjoint(encoded_iri(value.iri())),
        IRComponent::LessThan(value) => ConstraintPlan::LessThan(encoded_iri(value.iri())),
        IRComponent::LessThanOrEquals(value) => {
            ConstraintPlan::LessThanOrEquals(encoded_iri(value.iri()))
        }
        IRComponent::Or(value) => {
            ConstraintPlan::Or(shape_ids(schema, labels, value.shapes())?.into_boxed_slice())
        }
        IRComponent::And(value) => {
            ConstraintPlan::And(shape_ids(schema, labels, value.shapes())?.into_boxed_slice())
        }
        IRComponent::Not(value) => ConstraintPlan::Not(shape_id(schema, labels, value.shape())?),
        IRComponent::Xone(value) => {
            ConstraintPlan::Xone(shape_ids(schema, labels, value.shapes())?.into_boxed_slice())
        }
        IRComponent::Node(value) => ConstraintPlan::Node(shape_id(schema, labels, value.shape())?),
        IRComponent::HasValue(value) => ConstraintPlan::HasValue(encoded_object(value.value())?),
        IRComponent::In(value) => ConstraintPlan::In(
            value
                .values()
                .iter()
                .map(encoded_object)
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
        ),
        IRComponent::QualifiedValueShape(value) => ConstraintPlan::QualifiedValueShape {
            shape: shape_id(schema, labels, value.shape())?,
            min_count: value
                .qualified_min_count()
                .map(|count| nonnegative(shape, "sh:qualifiedMinCount", count))
                .transpose()?,
            max_count: value
                .qualified_max_count()
                .map(|count| nonnegative(shape, "sh:qualifiedMaxCount", count))
                .transpose()?,
            disjoint: value.qualified_value_shapes_disjoint().unwrap_or(false),
            siblings: shape_ids(schema, labels, value.siblings())?.into_boxed_slice(),
        },
        IRComponent::Closed(value) => {
            if !value.is_closed() {
                return Ok(None);
            }
            ConstraintPlan::Closed {
                ignored_properties: value
                    .ignored_properties()
                    .iter()
                    .map(encoded_iri)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }
        }
        IRComponent::Deactivated(_) => return Ok(None),
        IRComponent::BasicSparql(_) => {
            return Err(ShaclError::UnsupportedComponent {
                shape: shape.0.clone(),
                component: "http://www.w3.org/ns/shacl#SPARQLConstraintComponent".to_owned(),
            }
            .into());
        }
    };
    Ok(Some(plan))
}

fn shape_ids(
    schema: &IRSchema,
    labels: &HashMap<EncodedTerm, ShapeId>,
    indexes: &[ShapeLabelIdx],
) -> Result<Vec<ShapeId>> {
    indexes
        .iter()
        .map(|index| shape_id(schema, labels, index))
        .collect()
}

fn shape_id(
    schema: &IRSchema,
    labels: &HashMap<EncodedTerm, ShapeId>,
    index: &ShapeLabelIdx,
) -> Result<ShapeId> {
    let shape = schema
        .get_shape_from_idx(index)
        .ok_or_else(|| ShaclError::IllFormedShapes {
            graph: "compiled schema".to_owned(),
            message: format!("shape index {index} is missing"),
        })?;
    let label = encoded_object(shape.id())?;
    labels.get(&label).copied().ok_or_else(|| {
        ShaclError::IllFormedShapes {
            graph: "compiled schema".to_owned(),
            message: format!("shape {} is missing from the compiled label map", label.0),
        }
        .into()
    })
}

fn compile_severity(severity: &Severity) -> SeverityPlan {
    match severity {
        Severity::Trace => SeverityPlan::Trace,
        Severity::Debug => SeverityPlan::Debug,
        Severity::Info => SeverityPlan::Info,
        Severity::Warning => SeverityPlan::Warning,
        Severity::Violation => SeverityPlan::Violation,
        Severity::Generic(iri) => SeverityPlan::Custom(encoded_iri(iri)),
    }
}

fn compile_messages(shape: &IRShape) -> Box<[MessagePlan]> {
    let mut messages = shape
        .message()
        .into_iter()
        .flat_map(|messages| messages.iter())
        .map(|(language, text)| MessagePlan {
            language: language.as_ref().map(ToString::to_string),
            text: text.clone(),
        })
        .collect::<Vec<_>>();
    messages
        .sort_by(|left, right| (&left.language, &left.text).cmp(&(&right.language, &right.text)));
    messages.into_boxed_slice()
}

fn nonnegative(shape: &EncodedTerm, component: &str, value: isize) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        ShaclError::IllFormedShapes {
            graph: shape.0.clone(),
            message: format!("{component} must not be negative"),
        }
        .into()
    })
}

fn encoded_iri(iri: &IriS) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(iri.as_str()))
}

fn encoded_object(object: &Object) -> Result<EncodedTerm> {
    match object {
        Object::Iri(iri) => Ok(encoded_iri(iri)),
        Object::BlankNode(label) => BlankNode::new(label.clone())
            .map(|node| EncodedTerm::from_non_star_term(&Term::BlankNode(node)))
            .map_err(|error| {
                ShaclError::IllFormedShapes {
                    graph: "Rudof term conversion".to_owned(),
                    message: error.to_string(),
                }
                .into()
            }),
        Object::Literal(literal) => encoded_literal(literal),
        Object::Triple { .. } => Err(ShaclError::UnsupportedRdfStarTerm {
            term: object.to_string(),
        }
        .into()),
    }
}

fn encoded_literal(
    literal: &rudof_rdf::rdf_core::term::literal::ConcreteLiteral,
) -> Result<EncodedTerm> {
    let value = if let Some(language) = literal.lang() {
        Literal::new_language_tagged_literal(literal.lexical_form(), language.to_string()).map_err(
            |error| ShaclError::IllFormedShapes {
                graph: "Rudof literal conversion".to_owned(),
                message: error.to_string(),
            },
        )?
    } else {
        let datatype = literal.datatype();
        let datatype = datatype
            .get_iri()
            .map_err(|error| ShaclError::IllFormedShapes {
                graph: "Rudof literal conversion".to_owned(),
                message: error.to_string(),
            })?;
        Literal::new_typed_literal(
            literal.lexical_form(),
            NamedNode::new_unchecked(datatype.as_str()),
        )
    };
    Ok(EncodedTerm::from_non_star_term(&Term::Literal(value)))
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEstimate, auto_delta};

    #[test]
    fn auto_tie_guard() {
        let saturated = ExecutionEstimate {
            delta_work: u64::MAX,
            full_work: u64::MAX,
            affected_shapes: 0,
            focus_nodes: 0,
            full_required: false,
        };
        assert!(!auto_delta(true, saturated));

        let finite = ExecutionEstimate {
            delta_work: 4,
            full_work: 4,
            ..saturated
        };
        assert!(auto_delta(true, finite));
    }
}
