use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io;
use std::sync::Arc;

use crate::core::{Batch, EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use crate::replication::ReplicationEngine;
use crate::store::{EncodedQuad, GraphSubjectPredicate, PageCursor, PageRequest, TermId};
use oxjsonld::{JsonLdParser, JsonLdRemoteDocument};
use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use rocraters::ro_crate::constraints::{DataType, EntityValue, Id, License};
use rocraters::ro_crate::context::RoCrateContext;
use rocraters::ro_crate::contextual_entity::ContextualEntity;
use rocraters::ro_crate::data_entity::DataEntity;
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use rocraters::ro_crate::rdf::{
    ContextResolverBuilder, ConversionOptions, RdfError, rocrate_to_rdf_with_options,
};
use rocraters::ro_crate::rocrate::RoCrate;
use rocraters::ro_crate::root::RootDataEntity;

const ROCRATE_1_1_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.1/context";
const ROCRATE_1_2_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.2/context";
const WORKFLOW_RUN_CONTEXT_URL: &str = "https://w3id.org/ro/terms/workflow-run/context";
const ROCRATE_1_1_SPEC_URL: &str = "https://w3id.org/ro/crate/1.1";
const ROCRATE_1_2_SPEC_URL: &str = "https://w3id.org/ro/crate/1.2";
const ROCRATE_CONTEXT_URL: &str = ROCRATE_1_2_CONTEXT_URL;
const ROCRATE_SPEC_URL: &str = ROCRATE_1_2_SPEC_URL;
const JSONLD_BASE_IRI: &str = "https://craqle.invalid/";
const ROCRATE_1_1_CONTEXT: &[u8] = include_bytes!("../resources/ro_crate_1_1.jsonld");
const ROCRATE_1_2_CONTEXT: &[u8] = include_bytes!("../resources/ro_crate_1_2.jsonld");
const WORKFLOW_RUN_CONTEXT: &[u8] = include_bytes!("../resources/workflow_run.jsonld");
const XSD_BOOLEAN_IRI: &str = "http://www.w3.org/2001/XMLSchema#boolean";
const XSD_DOUBLE_IRI: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_INTEGER_IRI: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_STRING_IRI: &str = "http://www.w3.org/2001/XMLSchema#string";
const DCTERMS_CONFORMS_TO_IRI: &str = "http://purl.org/dc/terms/conformsTo";
const PROF_HAS_ARTIFACT_IRI: &str = "http://www.w3.org/ns/dx/prof#hasArtifact";
const PROF_RESOURCE_DESCRIPTOR_IRI: &str = "http://www.w3.org/ns/dx/prof#ResourceDescriptor";
const METADATA_ID: &str = "ro-crate-metadata.json";
type TripleKey = (EncodedTerm, EncodedTerm, EncodedTerm);

fn root_id(graph_id: &GraphId) -> &str {
    graph_id.as_str()
}

fn root_term(graph_id: &GraphId) -> EncodedTerm {
    EncodedTerm::from_named_node(&graph_id.0)
}

fn crate_conforms_to() -> NamedNode {
    NamedNode::new_unchecked(DCTERMS_CONFORMS_TO_IRI)
}

enum AppendLikeCheckError {
    Store(crate::store::StoreError),
    NeedsFullDiff,
}

impl From<crate::store::StoreError> for AppendLikeCheckError {
    fn from(value: crate::store::StoreError) -> Self {
        Self::Store(value)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RoCrateError {
    #[error("update: {0}")]
    Update(#[from] crate::replication::UpdateError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("rdf: {0}")]
    Rdf(#[from] RdfError),
    #[error("json-ld: {0}")]
    JsonLd(String),
    #[error("invalid graph: {0}")]
    InvalidGraph(String),
    #[error("entity not found: {0}")]
    EntityNotFound(String),
    #[error("unsupported JSON-LD shape: {0}")]
    UnsupportedJsonLd(String),
    #[error("unsupported compact IRI or property name: {0}")]
    UnsupportedTerm(String),
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
}

/// Cursor-style JSON-LD page export returned by partial RO-Crate export APIs.
#[derive(Debug, Clone)]
pub struct RoCratePage {
    pub jsonld: String,
    pub total_data_entities: usize,
    pub returned_data_entities: usize,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalJsonLd {
    pub nquads: String,
    pub digest: [u8; 32],
}

pub fn canonicalize_jsonld(jsonld: &str) -> Result<CanonicalJsonLd, RoCrateError> {
    let value: serde_json::Value = serde_json::from_str(jsonld)?;
    canonicalize_value(&value)
}

pub fn validate_rocrate_jsonld(jsonld: &str) -> Result<CanonicalJsonLd, RoCrateError> {
    let value: serde_json::Value = serde_json::from_str(jsonld)?;
    validate_jsonld_import(&value)?;
    let graph_id = GraphId::new(JSONLD_BASE_IRI);
    let target = jsonld_triples(&graph_id, &value)?;
    let pointers = SubmittedPointers::new(&value, &graph_id);
    validate_crate_version(&graph_id, &target, &pointers)?;
    validate_complete_import_triples(&graph_id, &target, Some(&pointers))?;
    canonicalize_value(&value)
}

/// Per-operation view of one crate: the graph, its interned term id, and the
/// orphaned entities hidden from every read (G6).
///
/// Built once per public operation and threaded through every read beneath it.
/// The orphan set is snapshotted at operation start, which strengthens the old
/// behaviour rather than weakening it: every read within one operation now
/// agrees on the same visible set instead of straddling a concurrent commit.
struct CrateCtx {
    graph: GraphId,
    /// `None` when the graph term was never interned, i.e. the graph holds no
    /// quads. Every read below then yields nothing, as it did before.
    graph_tid: Option<TermId>,
    orphaned: HashSet<EncodedTerm>,
}

impl CrateCtx {
    fn root_id(&self) -> &str {
        root_id(&self.graph)
    }

    fn root_term(&self) -> EncodedTerm {
        root_term(&self.graph)
    }

    /// Is this term hidden from readers because it is an orphaned entity?
    fn hides(&self, term: &EncodedTerm) -> bool {
        self.orphaned.contains(term)
    }

    /// Drop `(predicate, object)` pairs whose object is orphaned — the object
    /// side of orphan hiding, preserving input order.
    fn retain_visible(
        &self,
        mut triples: Vec<(EncodedTerm, EncodedTerm)>,
    ) -> Vec<(EncodedTerm, EncodedTerm)> {
        triples.retain(|(_, object)| !self.hides(object));
        triples
    }
}

/// The three terms of a triple probed for liveness inside a [`CrateCtx`]'s graph.
struct TripleProbe<'a> {
    subject: &'a EncodedTerm,
    predicate: &'a EncodedTerm,
    object: &'a EncodedTerm,
}

/// A `hasPart` edge probed for existence.
struct HasPartLink<'a> {
    parent_id: &'a str,
    child_id: &'a str,
}

/// The triples one entity should carry after a write: its type, its name, and
/// any caller-supplied extras. `entity_id` is already normalized.
pub(crate) struct EntitySpec<'a> {
    pub entity_id: &'a str,
    pub entity_type: &'a str,
    pub name: &'a str,
    pub additional_triples: &'a [(NamedNode, Term)],
}

/// One entity to write together with the parent it hangs under.
struct EntityUpsert<'a> {
    parent_id: &'a str,
    entity: EntitySpec<'a>,
}

/// An incremental patch to one subject: the triples it should carry afterwards
/// plus any predicates to clear even when the patch supplies no value for them.
struct SubjectPatch<'a> {
    subject_id: &'a str,
    desired: Vec<(EncodedTerm, EncodedTerm)>,
    replaced_predicates: &'a [NamedNode],
}

/// A single property mutation on one entity.
///
/// `old_value: Some(v)` replaces only the triple matching `v`; `None` removes
/// **all** existing values for the predicate first (replace-all semantics).
pub(crate) struct PropertyUpdate<'a> {
    pub entity_id: &'a str,
    pub predicate: &'a str,
    pub old_value: Option<&'a str>,
    pub new_value: &'a str,
}

/// What an export renders: the page of root-linked data entities to inline,
/// and whether to pretty-print the result.
struct ExportRender<'a> {
    page_entities: &'a [EncodedTerm],
    pretty: bool,
}

/// Inputs the view builder needs beyond the crate context.
struct ExportView<'a> {
    page_entities: &'a [EncodedTerm],
    ctx: &'a ContextTermMap,
}

/// Everything needed to render the root data entity of an export.
struct RootExportView<'a> {
    graph_id: &'a GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm)>,
    /// The ids the root must declare as `hasPart`: every data entity this
    /// export emits. RO-Crate 1.2 requires each of them to be linked from the
    /// root, so an export that emits one without a link is not a valid crate.
    has_part: Vec<String>,
    ctx: &'a ContextTermMap,
}

/// The non-page entities a partial view emits.
///
/// Split because the two halves are linked differently: contextual entities
/// need no `hasPart`, while profile artifacts are `File`/`MediaObject` data
/// entities and so MUST hang off the root's `hasPart`.
#[derive(Default)]
struct PartialViewEntities {
    contextual: BTreeSet<String>,
    artifacts: BTreeSet<String>,
}

/// Everything needed to render the metadata descriptor of an export.
struct MetadataExportView<'a> {
    graph_id: &'a GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm)>,
    ctx: &'a ContextTermMap,
}

/// Description of one new entity to append during batch ingest.
#[derive(Debug, Clone)]
pub struct NewDataEntity {
    pub entity_id: String,
    pub entity_type: String,
    pub name: String,
    pub additional_triples: Vec<(NamedNode, Term)>,
}

/// Result of a batch append operation.
#[derive(Debug, Clone)]
pub struct AppendDataEntitiesReport {
    pub batch: Batch,
    pub entity_count: usize,
    pub change_count: usize,
}

/// RO-Crate lifecycle management built on the replication engine.
pub(crate) struct RoCrateManager {
    engine: Arc<ReplicationEngine>,
}

impl RoCrateManager {
    pub(crate) fn new(engine: Arc<ReplicationEngine>) -> Self {
        Self { engine }
    }

    /// Create a new RO-Crate with its base entities.
    pub(crate) fn create_crate(
        &self,
        graph_id: GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: Option<&str>,
    ) -> Result<Batch, RoCrateError> {
        let cx = self.crate_ctx(&graph_id)?;
        if self.graph_is_empty(&cx)? {
            let license_value = license.map(encoded_license_value).transpose()?;
            let changes = create_crate_scaffold_changes_with_license(
                &graph_id,
                name,
                description,
                date_published,
                license_value,
            );
            return Ok(self.engine.local_apply_changes(&graph_id, changes)?);
        }

        let changes = match license {
            Some(license) => {
                let mut rocrate = create_crate_rocrate_with_license(
                    &graph_id,
                    name,
                    description,
                    date_published,
                    license_from_str(license)?,
                );
                self.plan_rocrate_replacement(&cx, &mut rocrate)?
            }
            None => {
                let target =
                    triples_from_insert_changes(&create_crate_scaffold_changes_with_license(
                        &graph_id,
                        name,
                        description,
                        date_published,
                        None,
                    ));
                validate_complete_import_triples(&graph_id, &target, None)?;
                diff_triples(&graph_id, &self.current_triples(&cx)?, &target)?
            }
        };
        let batch = self.engine.local_apply_changes(&graph_id, changes)?;
        self.reset_context_after_replacement(&graph_id)?;
        Ok(batch)
    }

    /// Create-crate path for scaffold requests already validated at their
    /// origin. Skips post-state rule validation; scaffold output is
    /// structurally valid by construction.
    pub(crate) fn create_crate_prevalidated(
        &self,
        graph_id: GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: Option<&str>,
    ) -> Result<Batch, RoCrateError> {
        let cx = self.crate_ctx(&graph_id)?;
        let is_replacement = !self.graph_is_empty(&cx)?;
        let changes = if is_replacement {
            match license {
                Some(license) => {
                    let mut rocrate = create_crate_rocrate_with_license(
                        &graph_id,
                        name,
                        description,
                        date_published,
                        license_from_str(license)?,
                    );
                    self.plan_rocrate_replacement(&cx, &mut rocrate)?
                }
                None => {
                    let target =
                        triples_from_insert_changes(&create_crate_scaffold_changes_with_license(
                            &graph_id,
                            name,
                            description,
                            date_published,
                            None,
                        ));
                    diff_triples(&graph_id, &self.current_triples(&cx)?, &target)?
                }
            }
        } else {
            create_crate_scaffold_changes_with_license(
                &graph_id,
                name,
                description,
                date_published,
                license.map(encoded_license_value).transpose()?,
            )
        };
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, changes)?;
        self.engine.rebuild_graph_diagnostics(&graph_id)?;
        if is_replacement {
            // Keep checked and prevalidated replacement paths in lockstep.
            self.reset_context_after_replacement(&graph_id)?;
        }
        Ok(batch)
    }

    /// Validate and materialize the changes for creating a crate without applying them.
    pub(crate) fn validate_create_crate(
        &self,
        graph_id: &GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: Option<&str>,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        let current = self.current_triples(&cx)?;
        if current.is_empty() {
            let changes = create_crate_scaffold_changes_with_license(
                graph_id,
                name,
                description,
                date_published,
                license.map(encoded_license_value).transpose()?,
            );
            let target = triples_from_insert_changes(&changes);
            validate_complete_import_triples(graph_id, &target, None)?;
            return Ok(changes);
        }

        let target = match license {
            Some(license) => {
                let mut rocrate = create_crate_rocrate_with_license(
                    graph_id,
                    name,
                    description,
                    date_published,
                    license_from_str(license)?,
                );
                normalize_rocrate(&mut rocrate);
                rocrate_triples(&rocrate)?
            }
            None => triples_from_insert_changes(&create_crate_scaffold_changes_with_license(
                graph_id,
                name,
                description,
                date_published,
                None,
            )),
        };
        validate_complete_import_triples(graph_id, &target, None)?;
        diff_triples(graph_id, &current, &target)
    }

    /// Add a data entity with automatic hasPart linkage from root.
    pub(crate) fn add_data_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        let entity_id = normalize_entity_id(entity_id);
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;
        self.upsert_data_entity_incremental(
            &cx,
            EntityUpsert {
                parent_id: cx.root_id(),
                entity: EntitySpec {
                    entity_id: &entity_id,
                    entity_type,
                    name,
                    additional_triples: &additional_triples,
                },
            },
        )
    }

    pub(crate) fn patch_data_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
        replaced_predicates: &[NamedNode],
    ) -> Result<Batch, RoCrateError> {
        let entity_id = normalize_entity_id(entity_id);
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;
        let parent_id = cx.root_id();
        if !self.visible_subject_exists(&cx, parent_id)? {
            return Err(RoCrateError::EntityNotFound(parent_id.to_string()));
        }
        let mut changes = self.patch_subject_changes(
            &cx,
            SubjectPatch {
                subject_id: &entity_id,
                desired: entity_subject_triples(&EntitySpec {
                    entity_id: &entity_id,
                    entity_type,
                    name,
                    additional_triples: &additional_triples,
                })?,
                replaced_predicates,
            },
        )?;
        if !self.has_part_link(
            &cx,
            HasPartLink {
                parent_id,
                child_id: &entity_id,
            },
        )? {
            changes.push(insert_change(
                graph_id,
                parent_id,
                &vocab::schema_has_part(),
                encoded_subject(&entity_id),
            ));
        }
        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    pub(crate) fn append_new_root_data_entities(
        &self,
        graph_id: &GraphId,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport, RoCrateError> {
        self.append_new_data_entities_under(graph_id, root_id(graph_id), entities)
    }

    pub(crate) fn append_new_data_entities_under(
        &self,
        graph_id: &GraphId,
        parent_id: &str,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;

        let parent_id = normalize_entity_id(parent_id);
        if !self.visible_subject_exists(&cx, &parent_id)? {
            return Err(RoCrateError::EntityNotFound(parent_id));
        }

        let mut seen = HashSet::new();
        let mut changes = Vec::with_capacity(entities.len() * 6);
        for entity in entities {
            let entity_id = normalize_entity_id(&entity.entity_id);
            if !seen.insert(entity_id.clone()) {
                return Err(RoCrateError::InvalidBatch(format!(
                    "duplicate entity id `{entity_id}` in batch"
                )));
            }

            changes.push(insert_change(
                graph_id,
                &parent_id,
                &vocab::schema_has_part(),
                encoded_subject(&entity_id),
            ));

            for (predicate, object) in entity_subject_triples(&EntitySpec {
                entity_id: &entity_id,
                entity_type: &entity.entity_type,
                name: &entity.name,
                additional_triples: &entity.additional_triples,
            })? {
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph_id.clone(),
                    subject: encoded_subject(&entity_id),
                    predicate,
                    object,
                });
            }
        }

        let change_count = changes.len();
        let entity_count = seen.len();
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(graph_id, changes)?;
        // `additional_triples` may carry a `hasPart` edge that adopts an
        // existing orphan. That entity is never written, so only the orphan
        // record can return it to the search index (G7).
        self.engine.rebuild_graph_diagnostics(graph_id)?;
        Ok(AppendDataEntitiesReport {
            batch,
            entity_count,
            change_count,
        })
    }

    /// Add a contextual entity (no hasPart linkage needed).
    pub(crate) fn add_contextual_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;
        let entity_id = normalize_entity_id(entity_id);
        let changes = self.replace_subject_changes(
            &cx,
            &EntitySpec {
                entity_id: &entity_id,
                entity_type,
                name,
                additional_triples: &additional_triples,
            },
        )?;
        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    pub(crate) fn patch_contextual_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
        replaced_predicates: &[NamedNode],
    ) -> Result<Batch, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;
        let entity_id = normalize_entity_id(entity_id);
        let changes = self.patch_subject_changes(
            &cx,
            SubjectPatch {
                subject_id: &entity_id,
                desired: entity_subject_triples(&EntitySpec {
                    entity_id: &entity_id,
                    entity_type,
                    name,
                    additional_triples: &additional_triples,
                })?,
                replaced_predicates,
            },
        )?;
        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    /// Export a graph to RO-Crate JSON-LD.
    pub(crate) fn export_jsonld(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        // The full export is the same visible sequence as an unbounded page, so
        // both go through one implementation and cannot drift apart.
        let (_, page) = self.root_linked_data_entity_page(
            &cx,
            PageRequest {
                cursor: PageCursor::Offset(0),
                limit: usize::MAX,
            },
        )?;
        self.render_export_view(
            &cx,
            ExportRender {
                page_entities: &page,
                pretty: true,
            },
        )
    }

    /// Export a lightweight partial RO-Crate view without data entities.
    pub(crate) fn export_jsonld_summary(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        self.render_export_view(
            &cx,
            ExportRender {
                page_entities: &[],
                pretty: false,
            },
        )
    }

    /// Export an offset-based partial RO-Crate page of root-linked data entities.
    pub(crate) fn export_jsonld_page(
        &self,
        graph_id: &GraphId,
        offset: usize,
        limit: usize,
    ) -> Result<RoCratePage, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        let (total, page) = self.root_linked_data_entity_page(
            &cx,
            PageRequest {
                cursor: PageCursor::Offset(offset),
                limit,
            },
        )?;
        let jsonld = self.render_export_view(
            &cx,
            ExportRender {
                page_entities: &page,
                pretty: false,
            },
        )?;
        let returned = page.len();
        let has_more = offset + returned < total;
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_reference_value))
            .flatten();

        Ok(RoCratePage {
            jsonld,
            total_data_entities: total,
            returned_data_entities: returned,
            next_offset: has_more.then_some(offset + returned),
            next_cursor,
        })
    }

    /// Export a cursor-based partial RO-Crate page of root-linked data entities.
    pub(crate) fn export_jsonld_page_after(
        &self,
        graph_id: &GraphId,
        after_entity_id: Option<&str>,
        limit: usize,
    ) -> Result<RoCratePage, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        // Acceptor half of the cursor round trip: decode exactly what
        // [`encoded_reference_value`] emits. A page whose last entity is a blank node
        // hands back `_:b0`, and re-encoding that as the IRI `<_:b0>` matches no
        // interned term — `objects_page` then silently restarts from offset 0 and
        // the caller re-reads page one forever.
        let after = after_entity_id
            .map(normalize_entity_id)
            .map(|id| encoded_subject(&id));
        // One extra entry beyond `limit` is the has-more probe.
        let (total, mut page) = self.root_linked_data_entity_page(
            &cx,
            PageRequest {
                cursor: PageCursor::After(after.as_ref()),
                limit: limit.saturating_add(1),
            },
        )?;
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        let returned = page.len();
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_reference_value))
            .flatten();
        let jsonld = self.render_export_view(
            &cx,
            ExportRender {
                page_entities: &page,
                pretty: false,
            },
        )?;

        Ok(RoCratePage {
            jsonld,
            total_data_entities: total,
            returned_data_entities: returned,
            next_offset: None,
            next_cursor,
        })
    }

    /// Import a JSON-LD RO-Crate metadata file into a named graph.
    ///
    /// New or empty graphs use the trusted bootstrap fast path. Existing graphs
    /// use a validated full-document replacement path that diffs against the
    /// current graph state.
    pub(crate) fn import_jsonld(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        let context = extract_raw_context(&value);
        let license = extract_raw_license(&value);
        let cx = self.crate_ctx(&graph_id)?;
        let batch = if self.graph_is_missing_or_empty(&graph_id)? {
            self.import_jsonld_into_empty_graph_trusted(&graph_id, value)?
        } else {
            self.replace_jsonld_in_existing_graph(&cx, value)?
        };
        self.store_import_context(&graph_id, context, license)?;
        Ok(batch)
    }

    /// Strict import path that validates complete RO-Crate semantics even for
    /// new-graph bootstrap imports.
    pub(crate) fn import_jsonld_checked(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        let context = extract_raw_context(&value);
        let license = extract_raw_license(&value);
        let cx = self.crate_ctx(&graph_id)?;
        let changes = self.plan_import_value_checked(&cx, value)?;
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, changes)?;
        self.engine.rebuild_graph_diagnostics(&graph_id)?;
        self.store_import_context(&graph_id, context, license)?;
        Ok(batch)
    }

    /// Import path for documents already validated at their origin.
    ///
    /// Skips complete RO-Crate semantic validation but keeps replace/diff
    /// semantics, CRDT authoring, and structural JSON-LD error handling. Only
    /// callers replaying origin-validated documents may use this.
    pub(crate) fn import_jsonld_prevalidated(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        let context = extract_raw_context(&value);
        let license = extract_raw_license(&value);
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(&graph_id, &value)?;
        let cx = self.crate_ctx(&graph_id)?;
        let changes = if self.graph_is_missing_or_empty(&graph_id)? {
            insert_changes(&graph_id, target)
        } else {
            match self.append_like_replace_changes(&cx, &target)? {
                Some(changes) => changes,
                None => diff_triples(&graph_id, &self.current_triples(&cx)?, &target)?,
            }
        };
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, changes)?;
        self.engine.rebuild_graph_diagnostics(&graph_id)?;
        self.store_import_context(&graph_id, context, license)?;
        Ok(batch)
    }

    /// Fast path for trusted bootstrap imports into a new or empty graph.
    ///
    /// This skips semantic RO-Crate validation and current-state diffing, and
    /// is intended for callers that already trust the input document.
    pub(crate) fn bootstrap_jsonld_trusted(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch, RoCrateError> {
        if !self.graph_is_missing_or_empty(&graph_id)? {
            return Err(RoCrateError::InvalidGraph(format!(
                "trusted bootstrap requires graph `{}` to be new or empty",
                graph_id.as_str()
            )));
        }

        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        let context = extract_raw_context(&value);
        let license = extract_raw_license(&value);
        let batch = self.import_jsonld_into_empty_graph_trusted(&graph_id, value)?;
        self.store_import_context(&graph_id, context, license)?;
        Ok(batch)
    }

    /// Compute the canonical change set for replacing a graph with a JSON-LD RO-Crate.
    pub(crate) fn plan_import_jsonld(
        &self,
        graph_id: &GraphId,
        jsonld: &str,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        self.plan_import_value(&self.crate_ctx(graph_id)?, value)
    }

    /// Compute and validate the strict import change set without applying it.
    pub(crate) fn plan_import_jsonld_checked(
        &self,
        graph_id: &GraphId,
        jsonld: &str,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        self.plan_import_value_checked(&self.crate_ctx(graph_id)?, value)
    }

    /// Apply one property mutation to an entity. See [`PropertyUpdate`] for the
    /// `old_value` semantics.
    pub(crate) fn update_property(
        &self,
        graph_id: &GraphId,
        update: PropertyUpdate<'_>,
    ) -> Result<Batch, RoCrateError> {
        let cx = self.crate_ctx(graph_id)?;
        self.require_rocrate_initialized(&cx)?;
        let entity_id = normalize_entity_id(update.entity_id);
        // The same encoding `subject_triples` reads with. Wrapping a `_:b0` id as
        // the IRI `<_:b0>` made the read below succeed and the write below land on
        // a term no reader ever looks at.
        let subject = encoded_subject(&entity_id);
        let current = self.subject_triples(&cx, &entity_id)?;
        if current.is_empty() {
            return Err(RoCrateError::EntityNotFound(entity_id));
        }

        let property = normalize_property(update.predicate);
        let predicate_node = property_named_node(&property)?;
        let predicate_term = EncodedTerm::from_named_node(&predicate_node);
        let mut changes = Vec::new();

        match update.old_value {
            Some(old_value) => {
                let old_object = property_value_encoded(&property, old_value)?;
                if current
                    .iter()
                    .any(|(pred, obj)| pred == &predicate_term && obj == &old_object)
                {
                    changes.push(MaterializedQuadChange::Delete {
                        graph: graph_id.clone(),
                        subject: subject.clone(),
                        predicate: predicate_term.clone(),
                        object: old_object,
                    });
                }
            }
            None => {
                for (pred, obj) in &current {
                    if pred == &predicate_term {
                        changes.push(MaterializedQuadChange::Delete {
                            graph: graph_id.clone(),
                            subject: subject.clone(),
                            predicate: pred.clone(),
                            object: obj.clone(),
                        });
                    }
                }
            }
        }

        changes.push(MaterializedQuadChange::Insert {
            graph: graph_id.clone(),
            subject,
            predicate: predicate_term,
            object: property_value_encoded(&property, update.new_value)?,
        });

        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    /// Build the per-operation context: the graph's interned term id plus a
    /// snapshot of its orphan set. See [`CrateCtx`] for the consistency note.
    fn crate_ctx(&self, graph: &GraphId) -> Result<CrateCtx, RoCrateError> {
        let store = self.engine.store();
        let graph_tid = store.lookup_term(&root_term(graph))?;
        let diagnostics = match graph_tid {
            Some(graph_tid) => store.graph_diagnostics_by_id(graph_tid)?,
            None => crate::core::GraphDiagnostics::default(),
        };
        Ok(CrateCtx {
            graph: graph.clone(),
            graph_tid,
            // `encoded_subject`, not `encoded_identifier`: diagnostics store a
            // blank node as `_:b0`, and re-encoding that as the IRI `<_:b0>`
            // would leave an orphaned blank node visible to every reader (G6).
            orphaned: diagnostics
                .orphaned_entities
                .iter()
                .map(|entity_id| encoded_subject(entity_id))
                .collect(),
        })
    }

    fn graph_is_empty(&self, cx: &CrateCtx) -> Result<bool, RoCrateError> {
        Ok(self.current_triples(cx)?.is_empty())
    }

    fn require_rocrate_initialized(&self, cx: &CrateCtx) -> Result<(), RoCrateError> {
        if self.root_is_dataset(cx)? {
            Ok(())
        } else {
            Err(RoCrateError::InvalidGraph(format!(
                "graph `{}` is not initialized as an RO-Crate",
                cx.graph.as_str()
            )))
        }
    }

    /// Is `<root> rdf:type schema:Dataset` live and visible?
    ///
    /// Three term lookups plus one O(1) index probe. The previous shape answered
    /// this by decoding the root's entire `hasPart` fan-out, which made bulk
    /// ingest quadratic: a fixed 10-entity append cost 415µs at 200 existing
    /// entities and 13.9ms at 10,000 (W2).
    fn root_is_dataset(&self, cx: &CrateCtx) -> Result<bool, RoCrateError> {
        let root = cx.root_term();
        let dataset = EncodedTerm::from_named_node(&vocab::schema_dataset());
        // Orphan hiding, preserved exactly: `subject_triples` yields nothing for
        // an orphaned subject and drops pairs whose object is orphaned, so an
        // orphaned root (or, pathologically, an orphaned `schema:Dataset`) must
        // keep failing this check as it did before (G6).
        if cx.hides(&root) || cx.hides(&dataset) {
            return Ok(false);
        }
        let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
        self.triple_is_live(
            cx,
            TripleProbe {
                subject: &root,
                predicate: &rdf_type,
                object: &dataset,
            },
        )
    }

    /// O(1) probe for one live triple in the context's graph. Committed state
    /// only, like every other read here.
    fn triple_is_live(&self, cx: &CrateCtx, probe: TripleProbe<'_>) -> Result<bool, RoCrateError> {
        let Some(graph) = cx.graph_tid else {
            return Ok(false);
        };
        let store = self.engine.store();
        let (Some(subject), Some(predicate), Some(object)) = (
            store.lookup_term(probe.subject)?,
            store.lookup_term(probe.predicate)?,
            store.lookup_term(probe.object)?,
        ) else {
            return Ok(false);
        };
        Ok(store.contains_quad(EncodedQuad {
            graph,
            subject,
            predicate,
            object,
        }))
    }

    fn upsert_data_entity_incremental(
        &self,
        cx: &CrateCtx,
        upsert: EntityUpsert<'_>,
    ) -> Result<Batch, RoCrateError> {
        // O(1) visibility probe. The previous `subject_triples(parent).is_empty()`
        // decoded the parent's whole fan-out just to test emptiness (W2); this is
        // also exactly the check `append_new_data_entities_under` already made, so
        // the two entry points now agree on what "parent exists" means.
        if !self.visible_subject_exists(cx, upsert.parent_id)? {
            return Err(RoCrateError::EntityNotFound(upsert.parent_id.to_string()));
        }
        let mut changes = self.replace_subject_changes(cx, &upsert.entity)?;
        if !self.has_part_link(
            cx,
            HasPartLink {
                parent_id: upsert.parent_id,
                child_id: upsert.entity.entity_id,
            },
        )? {
            changes.push(insert_change(
                &cx.graph,
                upsert.parent_id,
                &vocab::schema_has_part(),
                encoded_subject(upsert.entity.entity_id),
            ));
        }
        Ok(self.engine.local_apply_changes(&cx.graph, changes)?)
    }

    /// Diff one subject's visible triples against exactly the triples `spec`
    /// describes: deletes for what is no longer wanted, inserts for what is new.
    fn replace_subject_changes(
        &self,
        cx: &CrateCtx,
        spec: &EntitySpec<'_>,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let subject = encoded_subject(spec.entity_id);
        let desired: BTreeSet<(EncodedTerm, EncodedTerm)> =
            entity_subject_triples(spec)?.into_iter().collect();
        let current: BTreeSet<(EncodedTerm, EncodedTerm)> = self
            .subject_triples(cx, spec.entity_id)?
            .into_iter()
            .collect();

        let mut changes = Vec::new();
        for (predicate, object) in current.difference(&desired) {
            changes.push(MaterializedQuadChange::Delete {
                graph: cx.graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            });
        }
        for (predicate, object) in desired.difference(&current) {
            changes.push(MaterializedQuadChange::Insert {
                graph: cx.graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            });
        }
        Ok(changes)
    }

    /// Incremental counterpart of [`Self::replace_subject_changes`]: only the
    /// predicates the patch mentions (its own, plus `replaced_predicates`) are
    /// cleared, so unrelated triples on the subject survive.
    fn patch_subject_changes(
        &self,
        cx: &CrateCtx,
        patch: SubjectPatch<'_>,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let subject = encoded_subject(patch.subject_id);
        let current: BTreeSet<(EncodedTerm, EncodedTerm)> = self
            .subject_triples(cx, patch.subject_id)?
            .into_iter()
            .collect();
        let desired: BTreeSet<(EncodedTerm, EncodedTerm)> = patch.desired.into_iter().collect();
        let mut replaced: BTreeSet<EncodedTerm> = desired
            .iter()
            .map(|(predicate, _)| predicate.clone())
            .collect();
        replaced.extend(
            patch
                .replaced_predicates
                .iter()
                .map(EncodedTerm::from_named_node),
        );

        let mut changes = Vec::new();
        for (predicate, object) in current.difference(&desired) {
            if replaced.contains(predicate) {
                changes.push(MaterializedQuadChange::Delete {
                    graph: cx.graph.clone(),
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                });
            }
        }
        for (predicate, object) in desired.difference(&current) {
            changes.push(MaterializedQuadChange::Insert {
                graph: cx.graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            });
        }
        Ok(changes)
    }

    /// The visible `(predicate, object)` pairs of one subject: nothing when the
    /// subject itself is orphaned, and never a pair pointing at an orphan (G6).
    fn subject_triples(
        &self,
        cx: &CrateCtx,
        subject_id: &str,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
        let (Some(graph_tid), Some(subject_tid)) =
            (cx.graph_tid, self.visible_subject_tid(cx, subject_id)?)
        else {
            return Ok(Vec::new());
        };
        Ok(cx.retain_visible(
            self.engine
                .store()
                .triples_for_subject(graph_tid, subject_tid)?,
        ))
    }

    /// The root's visible triples minus its `hasPart` fan-out, which every
    /// export pages separately. Filtering by predicate id happens inside the
    /// store, before anything is decoded.
    fn root_triples_excluding_has_part(
        &self,
        cx: &CrateCtx,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
        let (Some(graph_tid), Some(subject_tid)) =
            (cx.graph_tid, self.visible_subject_tid(cx, cx.root_id())?)
        else {
            return Ok(Vec::new());
        };
        let store = self.engine.store();
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let triples = match store.lookup_term(&has_part)? {
            Some(excluded) => {
                store.triples_for_subject_excluding_predicate(graph_tid, subject_tid, excluded)?
            }
            None => store.triples_for_subject(graph_tid, subject_tid)?,
        };
        Ok(cx.retain_visible(triples))
    }

    /// The interned id of `subject_id`, or `None` when the subject is hidden by
    /// the orphan set or was never interned.
    ///
    /// `encoded_subject`, not `encoded_identifier`: JSON-LD import mints blank
    /// nodes for inline nested entities, and a `_:b0` subject must not be
    /// re-encoded as the IRI `<_:b0>` or its triples become unreadable.
    fn visible_subject_tid(
        &self,
        cx: &CrateCtx,
        subject_id: &str,
    ) -> Result<Option<TermId>, RoCrateError> {
        let subject = encoded_subject(subject_id);
        if cx.hides(&subject) {
            return Ok(None);
        }
        Ok(self.engine.store().lookup_term(&subject)?)
    }

    fn has_part_link(&self, cx: &CrateCtx, link: HasPartLink<'_>) -> Result<bool, RoCrateError> {
        let parent = encoded_subject(link.parent_id);
        let child = encoded_subject(link.child_id);
        // Same orphan hiding the old `subject_triples`-based check had: an
        // orphaned parent exposes no triples at all, and an orphaned child is
        // filtered out of its parent's objects, so either end being orphaned
        // keeps the link invisible (G6).
        if cx.hides(&parent) || cx.hides(&child) {
            return Ok(false);
        }
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        self.triple_is_live(
            cx,
            TripleProbe {
                subject: &parent,
                predicate: &has_part,
                object: &child,
            },
        )
    }

    fn build_partial_export_view(
        &self,
        cx: &CrateCtx,
        view: ExportView<'_>,
    ) -> Result<RoCrate, RoCrateError> {
        let metadata = export_metadata_descriptor(MetadataExportView {
            graph_id: &cx.graph,
            triples: self.subject_triples(cx, METADATA_ID)?,
            ctx: view.ctx,
        })?;

        let extra = self.collect_partial_view_entities(cx, view.page_entities)?;
        let mut has_part: Vec<String> = view
            .page_entities
            .iter()
            .filter_map(encoded_reference_value)
            .collect();
        let mut entities = Vec::with_capacity(extra.contextual.len() + extra.artifacts.len());
        for subject_id in extra.contextual.iter().chain(extra.artifacts.iter()) {
            let triples = self.subject_triples(cx, subject_id)?;
            if triples.is_empty() {
                continue;
            }
            // Only artifacts that made it into the document get a link, so the
            // root never points at an entity this export does not describe.
            if extra.artifacts.contains(subject_id) {
                has_part.push(subject_id.clone());
            }
            entities.push(export_graph_entity(subject_id, triples, view.ctx)?);
        }

        let root = export_root_entity(RootExportView {
            graph_id: &cx.graph,
            triples: self.root_triples_excluding_has_part(cx)?,
            has_part,
            ctx: view.ctx,
        })?;

        let mut graph = vec![
            GraphVector::MetadataDescriptor(metadata),
            GraphVector::RootDataEntity(root),
        ];
        graph.extend(entities);

        for entity in view.page_entities {
            let Some(subject_id) = encoded_reference_value(entity) else {
                continue;
            };
            if subject_id == cx.root_id() || subject_id == METADATA_ID {
                continue;
            }
            let triples = self.subject_triples(cx, &subject_id)?;
            if triples.is_empty() {
                continue;
            }
            graph.push(export_graph_entity(&subject_id, triples, view.ctx)?);
        }

        Ok(RoCrate {
            context: default_context(),
            graph,
        })
    }

    /// Render an export view to a JSON-LD string, splicing the graph's stored
    /// raw `@context` back in when one exists. When no custom context is stored,
    /// output matches the bare default RO-Crate context byte-for-byte.
    fn render_export_view(
        &self,
        cx: &CrateCtx,
        render: ExportRender<'_>,
    ) -> Result<String, RoCrateError> {
        let raw_context = self.engine.store().graph_context(&cx.graph)?;
        let raw_license = match self.engine.store().graph_license(&cx.graph)? {
            Some((raw, digest)) if digest == self.graph_digest(cx)? => {
                Some(serde_json::from_str(&raw)?)
            }
            _ => None,
        };
        let ctx = ContextTermMap::from_raw(raw_context.as_deref());
        let rocrate = self.build_partial_export_view(
            cx,
            ExportView {
                page_entities: render.page_entities,
                ctx: &ctx,
            },
        )?;
        let mut document = serde_json::to_value(&rocrate)?;
        let license_values = self
            .subject_triples(cx, cx.root_id())?
            .into_iter()
            .filter(|(predicate, _)| {
                predicate == &EncodedTerm::from_named_node(&vocab::schema_license())
            })
            .map(|(_, object)| serde_json::to_value(entity_value_from_encoded_term(&object)))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(root) = document
            .get_mut("@graph")
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|entries| {
                entries.iter_mut().find(|entry| {
                    entry.get("@id").and_then(serde_json::Value::as_str) == Some(cx.root_id())
                })
            })
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(raw_license) = raw_license {
                root.insert("license".to_string(), raw_license);
            } else {
                match license_values.len() {
                    0 => {
                        root.remove("license");
                    }
                    1 => {
                        root.insert("license".to_string(), license_values[0].clone());
                    }
                    _ => {
                        root.insert(
                            "license".to_string(),
                            serde_json::Value::Array(license_values),
                        );
                    }
                }
            }
        }
        match raw_context {
            None if render.pretty => Ok(serde_json::to_string_pretty(&document)?),
            None => Ok(serde_json::to_string(&document)?),
            Some(raw) => splice_context_json(document, &raw, render.pretty),
        }
    }

    /// A full crate replacement declares only the default RO-Crate context, so
    /// any custom context left over from a prior import is now stale. Revert it
    /// through the same store+publish path as an import (last write wins),
    /// which no-ops when there is nothing custom to clear. Every full
    /// replacement path (checked and prevalidated create alike) must call this
    /// after a successful apply.
    fn reset_context_after_replacement(&self, graph_id: &GraphId) -> Result<(), RoCrateError> {
        self.store_import_context(graph_id, None, None)
    }

    fn plan_import_value(
        &self,
        cx: &CrateCtx,
        value: serde_json::Value,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let graph_id = &cx.graph;
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(graph_id, &value)?;
        let graph_exists = match cx.graph_tid {
            Some(graph_tid) => self.engine.store().contains_graph_by_id(graph_tid)?,
            None => false,
        };
        if !graph_exists {
            return Ok(insert_changes(graph_id, target));
        }

        let current = self.current_triples(cx)?;
        if current.is_empty() {
            return Ok(insert_changes(graph_id, target));
        }

        diff_triples(graph_id, &current, &target)
    }

    fn plan_import_value_checked(
        &self,
        cx: &CrateCtx,
        value: serde_json::Value,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let graph_id = &cx.graph;
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(graph_id, &value)?;
        let pointers = SubmittedPointers::new(&value, graph_id);
        validate_crate_version(graph_id, &target, &pointers)?;
        validate_complete_import_triples(graph_id, &target, Some(&pointers))?;
        if self.graph_is_missing_or_empty(graph_id)? {
            return Ok(insert_changes(graph_id, target));
        }

        match self.append_like_replace_changes(cx, &target)? {
            Some(changes) => Ok(changes),
            None => diff_triples(graph_id, &self.current_triples(cx)?, &target),
        }
    }

    fn graph_is_missing_or_empty(&self, graph_id: &GraphId) -> Result<bool, RoCrateError> {
        Ok(self.engine.store().graph_is_empty(graph_id)?)
    }

    fn replace_jsonld_in_existing_graph(
        &self,
        cx: &CrateCtx,
        value: serde_json::Value,
    ) -> Result<Batch, RoCrateError> {
        let graph_id = &cx.graph;
        let changes = self.plan_import_value_checked(cx, value)?;
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(graph_id, changes)?;
        self.engine.rebuild_graph_diagnostics(graph_id)?;
        Ok(batch)
    }

    fn import_jsonld_into_empty_graph_trusted(
        &self,
        graph_id: &GraphId,
        value: serde_json::Value,
    ) -> Result<Batch, RoCrateError> {
        let target = jsonld_triples(graph_id, &value)?;
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(graph_id, insert_changes(graph_id, target))?;
        self.engine.rebuild_graph_diagnostics(graph_id)?;
        Ok(batch)
    }

    /// Persist, and replicate when sync is configured, the render hints captured
    /// from an import. Last-write-wins; only writes when a hint changed.
    ///
    /// Phase 2 of a two-phase import: the quads are already committed. A failure
    /// here leaves the hints unchanged and self-heals, because re-importing the
    /// same document produces an empty quad diff while the hints still differ, so
    /// the store/publish is retried. Publish-first (G4), with a tag minted strictly
    /// above the stored one (G5), so the retry also wins over what it is healing.
    fn store_import_context(
        &self,
        graph_id: &GraphId,
        context: Option<String>,
        license: Option<String>,
    ) -> Result<(), RoCrateError> {
        let current = self.engine.store().graph_context(graph_id)?;
        // Built fresh, not from the caller's operation context: this runs after
        // the write, so the digest must describe the state the licence now
        // annotates, not the state the operation started from.
        let license_digest = match license.as_ref() {
            Some(_) => Some(self.graph_digest(&self.crate_ctx(graph_id)?)?),
            None => None,
        };
        let current_license = self.engine.store().graph_license(graph_id)?;
        if current == context && current_license == license.clone().zip(license_digest) {
            return Ok(());
        }
        if current.is_some() {
            tracing::warn!(
                graph = %graph_id.as_str(),
                "replacing stored RO-Crate @context for graph (last write wins)"
            );
        }
        self.engine
            .set_graph_context(graph_id, context, license, license_digest)?;
        Ok(())
    }

    fn plan_rocrate_replacement(
        &self,
        cx: &CrateCtx,
        rocrate: &mut RoCrate,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        normalize_rocrate(rocrate);
        diff_triples(
            &cx.graph,
            &self.current_triples(cx)?,
            &rocrate_triples(rocrate)?,
        )
    }

    fn current_triples(&self, cx: &CrateCtx) -> Result<BTreeSet<TripleKey>, RoCrateError> {
        let store = self.engine.store();
        let Some(graph_tid) = cx.graph_tid else {
            return Ok(BTreeSet::new());
        };

        let mut triples = BTreeSet::new();
        let mut term_cache = HashMap::new();
        store.for_each_quad_in_graph::<crate::store::StoreError, _>(graph_tid, |quad| {
            let subject = store.decode_term_cached(&mut term_cache, quad.subject)?;
            let predicate = store.decode_term_cached(&mut term_cache, quad.predicate)?;
            let object = store.decode_term_cached(&mut term_cache, quad.object)?;
            if !cx.hides(&subject) && !cx.hides(&object) {
                triples.insert((subject, predicate, object));
            }
            Ok(())
        })?;
        Ok(triples)
    }

    fn graph_digest(&self, cx: &CrateCtx) -> Result<[u8; 32], RoCrateError> {
        let mut hasher = blake3::Hasher::new();
        for (subject, predicate, object) in self.current_triples(cx)? {
            for term in [subject, predicate, object] {
                let bytes = term.0.as_bytes();
                hasher.update(&(bytes.len() as u64).to_be_bytes());
                hasher.update(bytes);
            }
        }
        Ok(*hasher.finalize().as_bytes())
    }

    fn append_like_replace_changes(
        &self,
        cx: &CrateCtx,
        target: &BTreeSet<TripleKey>,
    ) -> Result<Option<Vec<MaterializedQuadChange>>, RoCrateError> {
        let graph_id = &cx.graph;
        let store = self.engine.store();
        let Some(graph_tid) = cx.graph_tid else {
            return Ok(Some(insert_changes(graph_id, target.clone())));
        };

        let mut remaining = target.clone();
        let mut term_cache = HashMap::new();
        let append_like =
            match store.for_each_quad_in_graph::<AppendLikeCheckError, _>(graph_tid, |quad| {
                let subject = store.decode_term_cached(&mut term_cache, quad.subject)?;
                let predicate = store.decode_term_cached(&mut term_cache, quad.predicate)?;
                let object = store.decode_term_cached(&mut term_cache, quad.object)?;
                if cx.hides(&subject) || cx.hides(&object) {
                    return Ok(());
                }
                if !remaining.remove(&(subject, predicate, object)) {
                    return Err(AppendLikeCheckError::NeedsFullDiff);
                }
                Ok(())
            }) {
                Ok(()) => true,
                Err(AppendLikeCheckError::NeedsFullDiff) => false,
                Err(AppendLikeCheckError::Store(error)) => return Err(error.into()),
            };

        if append_like {
            Ok(Some(insert_changes(graph_id, remaining)))
        } else {
            Ok(None)
        }
    }

    fn visible_subject_exists(
        &self,
        cx: &CrateCtx,
        subject_id: &str,
    ) -> Result<bool, RoCrateError> {
        let subject = encoded_subject(subject_id);
        if cx.hides(&subject) {
            return Ok(false);
        }
        Ok(self.engine.store().contains_subject(&cx.graph, &subject)?)
    }

    /// One page of the root's *visible* `hasPart` objects, plus the visible total.
    ///
    /// With no orphans this is a straight `objects_page`. When the graph carries
    /// orphans the previous shape decoded and sorted the root's entire fan-out
    /// **per page** (R3); instead we walk the same ordered id sequence in
    /// windows, skipping hidden entries until `limit` visible ones are gathered.
    /// The visible sequence is identical either way because both paths take
    /// `objects_page`'s ordering, which sorts by decoded term string — the same
    /// order the old `sort()` over decoded objects produced.
    fn root_linked_data_entity_page(
        &self,
        cx: &CrateCtx,
        page: PageRequest<'_>,
    ) -> Result<(usize, Vec<EncodedTerm>), RoCrateError> {
        let store = self.engine.store();
        let root = cx.root_term();
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());

        if cx.orphaned.is_empty() {
            return Ok(store.objects_page(
                GraphSubjectPredicate {
                    graph: &cx.graph,
                    subject: &root,
                    predicate: &has_part,
                },
                page,
            )?);
        }

        // An orphaned root exposes no triples at all, so it links to nothing (G6).
        if cx.hides(&root) {
            return Ok((0, Vec::new()));
        }

        // Exact visible total without decoding the fan-out: count the raw objects
        // off the index, then subtract the orphans that are actually linked from
        // the root — one O(1) live-quad probe per orphan.
        let raw_total = store.count_objects_for_subject_predicate(&cx.graph, &root, &has_part)?;
        let mut hidden = 0usize;
        for orphan in &cx.orphaned {
            if self.triple_is_live(
                cx,
                TripleProbe {
                    subject: &root,
                    predicate: &has_part,
                    object: orphan,
                },
            )? {
                hidden += 1;
            }
        }
        let total = raw_total.saturating_sub(hidden);

        let limit = page.limit;
        if limit == 0 {
            return Ok((total, Vec::new()));
        }
        let (mut skip, start_after) = match page.cursor {
            PageCursor::Offset(offset) => (offset, None),
            PageCursor::After(after) => (0, after),
        };
        // Enough headroom that a single window can still yield `limit` visible
        // entries even if every hidden entry happens to fall inside it.
        let step = limit.saturating_add(hidden);

        let mut visible = Vec::new();
        let mut anchor: Option<EncodedTerm> = None;
        loop {
            let cursor = match &anchor {
                Some(term) => PageCursor::After(Some(term)),
                None => PageCursor::After(start_after),
            };
            let (_, window) = store.objects_page(
                GraphSubjectPredicate {
                    graph: &cx.graph,
                    subject: &root,
                    predicate: &has_part,
                },
                PageRequest {
                    cursor,
                    limit: step,
                },
            )?;
            let Some(last) = window.last().cloned() else {
                return Ok((total, visible));
            };
            for object in window {
                if cx.hides(&object) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                visible.push(object);
                if visible.len() == limit {
                    return Ok((total, visible));
                }
            }
            anchor = Some(last);
        }
    }

    fn collect_partial_view_entities(
        &self,
        cx: &CrateCtx,
        page_entities: &[EncodedTerm],
    ) -> Result<PartialViewEntities, RoCrateError> {
        let page_subjects: HashSet<String> = page_entities
            .iter()
            .filter_map(encoded_reference_value)
            .collect();
        let mut queue = VecDeque::from([METADATA_ID.to_string(), cx.root_id().to_string()]);
        queue.extend(page_subjects.iter().cloned());

        let mut expanded = HashSet::new();
        let mut collected = PartialViewEntities::default();
        let has_artifact =
            EncodedTerm::from_named_node(&NamedNode::new_unchecked(PROF_HAS_ARTIFACT_IRI));

        while let Some(subject_id) = queue.pop_front() {
            if !expanded.insert(subject_id.clone()) {
                continue;
            }

            let references = if subject_id == cx.root_id() {
                self.root_triples_excluding_has_part(cx)?
            } else {
                self.subject_triples(cx, &subject_id)?
            };
            let is_resource_descriptor =
                triples_have_type(&references, PROF_RESOURCE_DESCRIPTOR_IRI);

            for (predicate, object) in references {
                let Some(candidate_id) = encoded_reference_value(&object) else {
                    continue;
                };
                if candidate_id == cx.root_id()
                    || candidate_id == METADATA_ID
                    || page_subjects.contains(&candidate_id)
                {
                    continue;
                }

                let triples = self.subject_triples(cx, &candidate_id)?;
                if triples.is_empty() {
                    continue;
                }
                let is_profile_artifact = is_resource_descriptor
                    && predicate == has_artifact
                    && (triples_have_type(&triples, "File")
                        || triples_have_type(&triples, "MediaObject"));
                let inserted = if is_profile_artifact {
                    collected.artifacts.insert(candidate_id.clone())
                } else if triples_describe_contextual_entity(&triples) {
                    collected.contextual.insert(candidate_id.clone())
                } else {
                    continue;
                };

                if inserted {
                    queue.push_back(candidate_id);
                }
            }
        }

        Ok(collected)
    }
}

fn insert_change(
    graph_id: &GraphId,
    subject_id: &str,
    predicate: &NamedNode,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph_id.clone(),
        subject: encoded_subject(subject_id),
        predicate: EncodedTerm::from_named_node(predicate),
        object,
    }
}

fn create_crate_scaffold_changes_with_license(
    graph_id: &GraphId,
    name: &str,
    description: &str,
    date_published: &str,
    license_value: Option<EncodedTerm>,
) -> Vec<MaterializedQuadChange> {
    let root_id = root_id(graph_id);
    let mut changes = vec![
        insert_change(
            graph_id,
            METADATA_ID,
            &vocab::rdf_type(),
            EncodedTerm::from_named_node(&vocab::schema_creative_work()),
        ),
        insert_change(
            graph_id,
            METADATA_ID,
            &crate_conforms_to(),
            encoded_identifier(ROCRATE_SPEC_URL),
        ),
        insert_change(
            graph_id,
            METADATA_ID,
            &vocab::schema_about(),
            encoded_identifier(root_id),
        ),
        insert_change(
            graph_id,
            root_id,
            &vocab::rdf_type(),
            EncodedTerm::from_named_node(&vocab::schema_dataset()),
        ),
        insert_change(
            graph_id,
            root_id,
            &vocab::schema_name(),
            encoded_literal(name),
        ),
        insert_change(
            graph_id,
            root_id,
            &vocab::schema_description(),
            encoded_literal(description),
        ),
        insert_change(
            graph_id,
            root_id,
            &vocab::schema_date_published(),
            encoded_literal(date_published),
        ),
    ];
    if let Some(license_value) = license_value {
        changes.push(insert_change(
            graph_id,
            root_id,
            &vocab::schema_license(),
            license_value,
        ));
    }
    changes
}

fn create_crate_rocrate_with_license(
    graph_id: &GraphId,
    name: &str,
    description: &str,
    date_published: &str,
    license: License,
) -> RoCrate {
    let root_id = root_id(graph_id);
    RoCrate {
        context: default_context(),
        graph: vec![
            GraphVector::MetadataDescriptor(MetadataDescriptor {
                id: METADATA_ID.to_string(),
                type_: DataType::Term("CreativeWork".to_string()),
                conforms_to: Id::Id(ROCRATE_SPEC_URL.to_string()),
                about: Id::Id(root_id.to_string()),
                dynamic_entity: Some(HashMap::new()),
            }),
            GraphVector::RootDataEntity(RootDataEntity {
                id: root_id.to_string(),
                type_: DataType::Term("Dataset".to_string()),
                name: name.to_string(),
                description: description.to_string(),
                date_published: date_published.to_string(),
                license,
                dynamic_entity: Some(HashMap::new()),
            }),
        ],
    }
}

fn triples_from_insert_changes(changes: &[MaterializedQuadChange]) -> BTreeSet<TripleKey> {
    changes
        .iter()
        .filter_map(|change| match change {
            MaterializedQuadChange::Insert {
                subject,
                predicate,
                object,
                ..
            } => Some((subject.clone(), predicate.clone(), object.clone())),
            MaterializedQuadChange::Delete { .. } => None,
        })
        .collect()
}

fn diff_triples(
    graph_id: &GraphId,
    current: &BTreeSet<TripleKey>,
    target: &BTreeSet<TripleKey>,
) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
    let mut changes = Vec::new();
    for triple in current.difference(target) {
        changes.push(MaterializedQuadChange::Delete {
            graph: graph_id.clone(),
            subject: triple.0.clone(),
            predicate: triple.1.clone(),
            object: triple.2.clone(),
        });
    }
    for triple in target.difference(current) {
        changes.push(MaterializedQuadChange::Insert {
            graph: graph_id.clone(),
            subject: triple.0.clone(),
            predicate: triple.1.clone(),
            object: triple.2.clone(),
        });
    }
    Ok(changes)
}

fn insert_changes(graph_id: &GraphId, triples: BTreeSet<TripleKey>) -> Vec<MaterializedQuadChange> {
    triples
        .into_iter()
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph_id.clone(),
                subject,
                predicate,
                object,
            },
        )
        .collect()
}

#[derive(Default)]
struct SubmittedPointers {
    graph: String,
    entities: HashMap<String, String>,
    properties: HashMap<(String, String), String>,
}

impl SubmittedPointers {
    fn new(value: &serde_json::Value, graph_id: &GraphId) -> Self {
        let Some(document) = value.as_object() else {
            return Self::default();
        };
        let mut terms = HashMap::new();
        if let Some(context) = document.get("@context") {
            collect_context_terms(context, &mut terms, false);
        }
        let Some((graph_key, entries)) = document.iter().find_map(|(key, value)| {
            (key == "@graph" || key == "graph" || terms.get(key).is_some_and(|iri| iri == "@graph"))
                .then(|| value.as_array().map(|entries| (key, entries)))
                .flatten()
        }) else {
            return Self::default();
        };

        let graph = format!("/{}", escape_pointer(graph_key));
        let import_root = entries.iter().find_map(|entry| {
            let entity = entry.as_object()?;
            let id = submitted_id(entity, &terms)?;
            if normalize_entity_id(id) != METADATA_ID {
                return None;
            }
            entity.iter().find_map(|(key, value)| {
                (submitted_predicate(key, &terms).as_deref()
                    == Some(vocab::schema_about().as_str()))
                .then(|| reference_id(value, &terms))
                .flatten()
            })
        });

        let mut pointers = Self {
            graph,
            ..Self::default()
        };
        for (index, entry) in entries.iter().enumerate() {
            let Some(entity) = entry.as_object() else {
                continue;
            };
            let Some(id) = submitted_id(entity, &terms) else {
                continue;
            };
            let normalized = normalize_entity_id(id);
            // Keyed by the same term the change set carries, so a document that
            // spells a nested entity out as `"@id": "_:b0"` still resolves to its
            // own JSON pointer instead of falling back to the whole `@graph`.
            let entity_term = if import_root.as_deref() == Some(normalized.as_str()) {
                root_term(graph_id)
            } else {
                encoded_subject(&normalized)
            };
            let entity_pointer = format!("{}/{index}", pointers.graph);
            pointers
                .entities
                .insert(entity_term.0.clone(), entity_pointer.clone());

            for key in entity.keys() {
                let Some(predicate) = submitted_predicate(key, &terms) else {
                    continue;
                };
                pointers.properties.insert(
                    (entity_term.0.clone(), predicate),
                    format!("{entity_pointer}/{}", escape_pointer(key)),
                );
            }
        }
        pointers
    }

    fn entity(&self, entity: &EncodedTerm) -> String {
        self.entities
            .get(&entity.0)
            .cloned()
            .unwrap_or_else(|| self.graph.clone())
    }

    fn property(&self, entity: &EncodedTerm, predicate: &EncodedTerm) -> String {
        self.properties
            .get(&(entity.0.clone(), predicate_iri(predicate)))
            .cloned()
            .unwrap_or_else(|| self.entity(entity))
    }
}

fn submitted_id<'a>(
    entity: &'a serde_json::Map<String, serde_json::Value>,
    terms: &HashMap<String, String>,
) -> Option<&'a str> {
    entity.iter().find_map(|(key, value)| {
        (key == "@id" || key == "id" || terms.get(key).is_some_and(|iri| iri == "@id"))
            .then(|| value.as_str())
            .flatten()
    })
}

fn reference_id(value: &serde_json::Value, terms: &HashMap<String, String>) -> Option<String> {
    match value {
        serde_json::Value::String(id) => Some(normalize_entity_id(id)),
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| reference_id(value, terms))
        }
        serde_json::Value::Object(object) => submitted_id(object, terms).map(normalize_entity_id),
        _ => None,
    }
}

fn submitted_predicate(key: &str, terms: &HashMap<String, String>) -> Option<String> {
    if key == "@type" || key == "type" || terms.get(key).is_some_and(|iri| iri == "@type") {
        return Some(vocab::rdf_type().as_str().to_string());
    }
    if key.starts_with('@') || terms.get(key).is_some_and(|iri| iri.starts_with('@')) {
        return None;
    }
    let term = terms.get(key).map_or(key, String::as_str);
    if term == "conformsTo" {
        return Some(DCTERMS_CONFORMS_TO_IRI.to_string());
    }
    property_named_node(&normalize_property(term))
        .ok()
        .map(|predicate| predicate.as_str().to_string())
}

fn predicate_iri(predicate: &EncodedTerm) -> String {
    predicate
        .to_named_node()
        .map(|predicate| predicate.as_str().to_string())
        .unwrap_or_default()
}

fn violation_pointer(
    pointers: Option<&SubmittedPointers>,
    entity: &EncodedTerm,
    predicate: &EncodedTerm,
) -> String {
    pointers.map_or_else(String::new, |pointers| pointers.property(entity, predicate))
}

fn validate_crate_version(
    graph_id: &GraphId,
    triples: &BTreeSet<TripleKey>,
    pointers: &SubmittedPointers,
) -> Result<(), RoCrateError> {
    let metadata = EncodedTerm::from_named_node(&vocab::metadata_descriptor());
    let root = root_term(graph_id);
    let conforms_to = EncodedTerm::from_named_node(&crate_conforms_to());
    let candidates = triples
        .iter()
        .filter(|(subject, predicate, _)| {
            (subject == &metadata || subject == &root) && predicate == &conforms_to
        })
        .collect::<Vec<_>>();
    if candidates.iter().any(|(_, _, object)| {
        object.to_named_node().is_some_and(|version| {
            matches!(
                version.as_str(),
                ROCRATE_1_1_SPEC_URL | ROCRATE_1_2_SPEC_URL
            )
        })
    }) {
        return Ok(());
    }

    let (version, entity, pointer) = candidates.first().map_or_else(
        || {
            let entity = if pointers.entities.contains_key(&metadata.0) {
                metadata.clone()
            } else {
                root.clone()
            };
            (None, entity.clone(), pointers.entity(&entity))
        },
        |(subject, _, object)| {
            (
                object
                    .to_named_node()
                    .map(|version| version.as_str().to_string())
                    .or_else(|| Some(object.0.clone())),
                (*subject).clone(),
                pointers.property(subject, &conforms_to),
            )
        },
    );
    let entity_id = entity
        .to_named_node()
        .map(|entity| entity.as_str().to_string());
    Err(RoCrateError::Update(
        crate::replication::UpdateError::ValidationFailed(vec![
            crate::core::CrateViolation::unsupported_version(
                version.as_deref(),
                pointer,
                entity_id,
            ),
        ]),
    ))
}

fn validate_complete_import_triples(
    graph_id: &GraphId,
    triples: &BTreeSet<TripleKey>,
    pointers: Option<&SubmittedPointers>,
) -> Result<(), RoCrateError> {
    let root = root_term(graph_id);
    let metadata = EncodedTerm::from_named_node(&vocab::metadata_descriptor());
    let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
    let dataset = EncodedTerm::from_named_node(&vocab::schema_dataset());
    let creative_work = EncodedTerm::from_named_node(&vocab::schema_creative_work());
    let about = EncodedTerm::from_named_node(&vocab::schema_about());
    let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
    let media_object = EncodedTerm::from_named_node(&vocab::schema_media_object());
    let root_name = EncodedTerm::from_named_node(&vocab::schema_name());
    let root_description = EncodedTerm::from_named_node(&vocab::schema_description());
    let root_date_published = EncodedTerm::from_named_node(&vocab::schema_date_published());

    // Borrowed throughout: `triples` outlives every collection here, so nothing
    // needs cloning. `BTreeSet<&EncodedTerm>` orders exactly as
    // `BTreeSet<EncodedTerm>` did (`Ord` on `&T` delegates to `Ord` on `T`, i.e.
    // the inner `String`), so the lexicographically smallest untyped subject and
    // the smallest orphan reported below stay byte-identical.
    let mut subjects: BTreeSet<&EncodedTerm> = BTreeSet::new();
    let mut typed_subjects: HashSet<&EncodedTerm> = HashSet::new();
    let mut adjacency: HashMap<&EncodedTerm, Vec<&EncodedTerm>> = HashMap::new();
    let mut data_entities: BTreeSet<&EncodedTerm> = BTreeSet::new();
    let mut has_root_dataset = false;
    let mut has_metadata_type = false;
    let mut root_name_count = 0usize;
    let mut root_description_count = 0usize;
    let mut root_date_published_count = 0usize;

    for (subject, predicate, object) in triples {
        subjects.insert(subject);

        if subject == &root && predicate == &root_name {
            root_name_count += 1;
        }
        if subject == &root && predicate == &root_description {
            root_description_count += 1;
        }
        if subject == &root && predicate == &root_date_published {
            root_date_published_count += 1;
        }
        if predicate == &rdf_type {
            typed_subjects.insert(subject);
            if subject == &root && object == &dataset {
                has_root_dataset = true;
            }
            if subject == &metadata && object == &creative_work {
                has_metadata_type = true;
            }
            if subject != &root && (object == &dataset || object == &media_object) {
                data_entities.insert(subject);
            }
        }

        if predicate == &has_part {
            adjacency.entry(subject).or_default().push(object);
            if subject != &root {
                data_entities.insert(subject);
            }
            if object != &root {
                data_entities.insert(object);
            }
        }
    }

    let mut violations = Vec::new();
    let metadata_about_root = triples.contains(&(metadata.clone(), about.clone(), root.clone()));

    if !has_root_dataset {
        violations.push(crate::core::CrateViolation::missing_root(
            pointers.map_or("/@graph", |pointers| pointers.graph.as_str()),
        ));
    }
    if !(has_metadata_type && metadata_about_root) {
        violations.push(crate::core::CrateViolation::missing_descriptor(
            pointers.map_or("/@graph", |pointers| pointers.graph.as_str()),
        ));
    }
    if root_name_count < 1 {
        violations.push(crate::core::CrateViolation::missing_property(
            root_id(graph_id),
            "schema:name",
            violation_pointer(pointers, &root, &root_name),
        ));
    }
    if root_description_count < 1 {
        violations.push(crate::core::CrateViolation::missing_property(
            root_id(graph_id),
            "schema:description",
            violation_pointer(pointers, &root, &root_description),
        ));
    }
    if root_date_published_count < 1 {
        violations.push(crate::core::CrateViolation::missing_property(
            root_id(graph_id),
            "schema:datePublished",
            violation_pointer(pointers, &root, &root_date_published),
        ));
    }
    if root_date_published_count != 1 {
        violations.push(crate::core::CrateViolation::invalid_date(
            root_date_published_count,
            violation_pointer(pointers, &root, &root_date_published),
        ));
    }

    if let Some(subject) = subjects
        .iter()
        .find(|subject| !typed_subjects.contains(*subject))
    {
        violations.push(crate::core::CrateViolation::missing_type(
            subject.0.clone(),
            violation_pointer(pointers, subject, &rdf_type),
        ));
    }

    let mut reachable: HashSet<&EncodedTerm> = HashSet::from([&root]);
    let mut queue: VecDeque<&EncodedTerm> = VecDeque::from([&root]);
    while let Some(current) = queue.pop_front() {
        if let Some(children) = adjacency.get(current) {
            for child in children {
                if reachable.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }

    if let Some(orphan) = data_entities
        .into_iter()
        .find(|entity| !reachable.contains(entity))
    {
        let pointer = violation_pointer(pointers, orphan, &has_part);
        violations.push(crate::core::CrateViolation::orphaned(
            orphan.0.clone(),
            pointer,
        ));
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(RoCrateError::Update(
            crate::replication::UpdateError::ValidationFailed(violations),
        ))
    }
}

fn validate_jsonld_import(value: &serde_json::Value) -> Result<(), RoCrateError> {
    let object = value.as_object().ok_or_else(|| {
        RoCrateError::UnsupportedJsonLd("top-level JSON-LD document must be an object".to_string())
    })?;

    let context = object
        .get("@context")
        .filter(|context| !context.is_null())
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(
                "RO-Crate import requires a non-null top-level `@context`".to_string(),
            )
        })?;
    let mut terms = HashMap::new();
    collect_context_terms(context, &mut terms, false);
    let graph = object
        .iter()
        .find_map(|(key, value)| {
            (key == "@graph" || key == "graph" || terms.get(key).is_some_and(|iri| iri == "@graph"))
                .then_some(value)
        })
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(
                "RO-Crate import requires a top-level `@graph` array".to_string(),
            )
        })?;

    let entries = graph.as_array().ok_or_else(|| {
        RoCrateError::UnsupportedJsonLd("`@graph` must be a JSON array".to_string())
    })?;

    for (index, entry) in entries.iter().enumerate() {
        let entity = entry.as_object().ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(format!("@graph entry {index} must be an object"))
        })?;
        if submitted_id(entity, &terms).is_none() {
            return Err(RoCrateError::UnsupportedJsonLd(format!(
                "@graph entry {index} must define string `@id`"
            )));
        }
    }

    Ok(())
}

fn canonicalize_value(value: &serde_json::Value) -> Result<CanonicalJsonLd, RoCrateError> {
    let mut lines = BTreeSet::new();
    for quad in jsonld_quads(value)? {
        lines.insert(format!("{quad} ."));
    }
    let nquads = if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.into_iter().collect::<Vec<_>>().join("\n"))
    };
    Ok(CanonicalJsonLd {
        digest: *blake3::hash(nquads.as_bytes()).as_bytes(),
        nquads,
    })
}

fn jsonld_quads(value: &serde_json::Value) -> Result<Vec<Quad>, RoCrateError> {
    // rocraters requires a root scalar license and string-only context maps, so
    // caller JSON-LD uses oxjsonld; typed internal crates still use rocraters.
    let mut prepared = value.clone();
    if let Some(object) = prepared.as_object_mut()
        && object
            .get("@context")
            .is_none_or(serde_json::Value::is_null)
    {
        object.insert(
            "@context".to_string(),
            serde_json::Value::String(ROCRATE_CONTEXT_URL.to_string()),
        );
    }
    let mut terms = HashMap::new();
    if let Some(context) = prepared.get("@context") {
        collect_context_terms(context, &mut terms, false);
    }
    label_blank_nodes(&mut prepared, "", true, &terms);
    let jsonld = serde_json::to_vec(&prepared)?;
    let parser = JsonLdParser::new()
        .with_base_iri(JSONLD_BASE_IRI)
        .map_err(|error| RoCrateError::JsonLd(error.to_string()))?
        .for_slice(&jsonld)
        .with_load_document_callback(|url, _| load_context(url));

    parser
        .map(|quad| quad.map_err(|error| RoCrateError::JsonLd(error.to_string())))
        .collect()
}

fn load_context(
    url: &str,
) -> Result<JsonLdRemoteDocument, Box<dyn std::error::Error + Send + Sync>> {
    let document = match url {
        ROCRATE_1_1_CONTEXT_URL => ROCRATE_1_1_CONTEXT,
        ROCRATE_1_2_CONTEXT_URL => ROCRATE_1_2_CONTEXT,
        WORKFLOW_RUN_CONTEXT_URL => WORKFLOW_RUN_CONTEXT,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unresolved offline JSON-LD context `{url}`"),
            )
            .into());
        }
    };
    Ok(JsonLdRemoteDocument {
        document: document.to_vec(),
        document_url: url.to_string(),
    })
}

fn label_blank_nodes(
    value: &mut serde_json::Value,
    pointer: &str,
    document: bool,
    terms: &HashMap<String, String>,
) {
    match value {
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                label_blank_nodes(value, &format!("{pointer}/{index}"), false, terms);
            }
        }
        serde_json::Value::Object(object) => {
            if !document
                && submitted_id(object, terms).is_none()
                && !object.contains_key("@value")
                && !object.contains_key("value")
                && !object.contains_key("@list")
                && !object.contains_key("@set")
                && object.keys().any(|key| {
                    key == "@type"
                        || key == "type"
                        || terms.get(key).is_some_and(|iri| iri == "@type")
                })
            {
                let suffix = blake3::hash(pointer.as_bytes()).to_hex();
                object.insert(
                    "@id".to_string(),
                    serde_json::Value::String(format!("_:b{}", &suffix[..32])),
                );
            }

            for (key, value) in object {
                if key == "@context" {
                    continue;
                }
                label_blank_nodes(
                    value,
                    &format!("{pointer}/{}", escape_pointer(key)),
                    false,
                    terms,
                );
            }
        }
        _ => {}
    }
}

fn escape_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn jsonld_triples(
    graph_id: &GraphId,
    value: &serde_json::Value,
) -> Result<BTreeSet<TripleKey>, RoCrateError> {
    let quads = jsonld_quads(value)?;
    let metadata = format!("{JSONLD_BASE_IRI}{METADATA_ID}");
    let about = vocab::schema_about();
    let import_root = quads.iter().find_map(|quad| {
        let NamedOrBlankNode::NamedNode(subject) = &quad.subject else {
            return None;
        };
        if subject.as_str() != metadata || quad.predicate != about {
            return None;
        }
        let Term::NamedNode(root) = &quad.object else {
            return None;
        };
        Some(root.as_str().to_string())
    });

    Ok(quads
        .into_iter()
        .map(|quad| {
            (
                remap_subject(quad.subject, import_root.as_deref(), graph_id),
                EncodedTerm::from_named_node(&quad.predicate),
                remap_object(quad.object, import_root.as_deref(), graph_id),
            )
        })
        .collect())
}

fn remap_subject(
    subject: NamedOrBlankNode,
    import_root: Option<&str>,
    graph_id: &GraphId,
) -> EncodedTerm {
    match subject {
        NamedOrBlankNode::NamedNode(node) => remap_node(node, import_root, graph_id),
        NamedOrBlankNode::BlankNode(node) => EncodedTerm(format!("_:{}", node.as_str())),
    }
}

fn remap_object(object: Term, import_root: Option<&str>, graph_id: &GraphId) -> EncodedTerm {
    match object {
        Term::NamedNode(node) => remap_node(node, import_root, graph_id),
        object => EncodedTerm::from_term(&object),
    }
}

fn remap_node(node: NamedNode, import_root: Option<&str>, graph_id: &GraphId) -> EncodedTerm {
    let iri = node.as_str();
    let mapped = if import_root == Some(iri) {
        graph_id.as_str().to_string()
    } else if let Some(relative) = iri.strip_prefix(JSONLD_BASE_IRI) {
        if relative == METADATA_ID {
            METADATA_ID.to_string()
        } else if relative.starts_with('#') {
            relative.to_string()
        } else {
            format!("./{relative}")
        }
    } else {
        iri.to_string()
    };
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(mapped))
}

fn entity_subject_triples(
    spec: &EntitySpec<'_>,
) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
    let mut triples = vec![
        (
            EncodedTerm::from_named_node(&vocab::rdf_type()),
            encoded_class_term(spec.entity_type)?,
        ),
        (
            EncodedTerm::from_named_node(&vocab::schema_name()),
            encoded_literal(spec.name),
        ),
    ];

    for (predicate, object) in spec.additional_triples {
        triples.push((
            EncodedTerm::from_named_node(predicate),
            EncodedTerm::from_term(object),
        ));
    }

    Ok(triples)
}

fn property_named_node(property: &str) -> Result<NamedNode, RoCrateError> {
    match property {
        "@type" | "type" => Ok(vocab::rdf_type()),
        "name" => Ok(vocab::schema_name()),
        "description" => Ok(vocab::schema_description()),
        "keywords" => Ok(vocab::schema_keywords()),
        "datePublished" => Ok(vocab::schema_date_published()),
        "license" => Ok(vocab::schema_license()),
        "about" => Ok(vocab::schema_about()),
        "conformsTo" => Ok(crate_conforms_to()),
        other if other.contains("://") => Ok(NamedNode::new_unchecked(other)),
        other if other.contains(':') => expand_known_compact_iri(other),
        other => Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{}",
            normalize_term(other)
        ))),
    }
}

fn property_value_encoded(property: &str, value: &str) -> Result<EncodedTerm, RoCrateError> {
    match property {
        "@type" | "type" => encoded_class_term(value),
        "license" | "about" | "conformsTo" => {
            if looks_like_identifier(value) {
                encoded_reference_term(value)
            } else {
                Ok(encoded_literal(value))
            }
        }
        _ => Ok(encoded_literal(value)),
    }
}

fn encoded_class_term(value: &str) -> Result<EncodedTerm, RoCrateError> {
    let iri = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else if value.contains(':') {
        expand_known_compact_iri(value)?.as_str().to_string()
    } else {
        format!("http://schema.org/{}", normalize_term(value))
    };
    Ok(EncodedTerm::from_named_node(&NamedNode::new_unchecked(
        &iri,
    )))
}

/// A value that is an IRI by construction — a constant or an id derived from the
/// graph name. Never reachable from a caller-supplied entity id; use
/// [`encoded_subject`] for those.
fn encoded_identifier(value: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(value))
}

/// An entity identifier, which import may have minted as a blank node.
///
/// Blank nodes are addressable entities in craqle: `oxjsonld` mints one for every
/// inline nested entity, and every reader — SPARQL, describe, search, export —
/// hands those ids back in bare `_:b0` form. So every caller-supplied id → term
/// conversion goes through here, on the write path as much as the read path.
/// Wrapping `_:b0` as the IRI `<_:b0>` yields a *different* term, which is how a
/// write could land somewhere no read would ever look (and how an orphaned blank
/// node stayed visible, G6).
fn encoded_subject(value: &str) -> EncodedTerm {
    EncodedTerm::from_subject_id(value)
}

fn encoded_literal(value: &str) -> EncodedTerm {
    EncodedTerm::from_term(&Term::Literal(oxrdf::Literal::new_simple_literal(value)))
}

fn encoded_reference_term(value: &str) -> Result<EncodedTerm, RoCrateError> {
    let is_identifier = value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || NamedNode::new(value).is_ok();

    if is_identifier {
        Ok(encoded_subject(value))
    } else if value.contains(':') {
        Ok(EncodedTerm::from_named_node(&expand_known_compact_iri(
            value,
        )?))
    } else {
        Err(RoCrateError::UnsupportedTerm(value.to_string()))
    }
}

fn encoded_license_value(license: &str) -> Result<EncodedTerm, RoCrateError> {
    if looks_like_identifier(license) {
        encoded_reference_term(license)
    } else {
        Ok(encoded_literal(license))
    }
}

fn expand_known_compact_iri(value: &str) -> Result<NamedNode, RoCrateError> {
    if let Some(local) = value.strip_prefix("schema:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdf:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdfs:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/2000/01/rdf-schema#{local}"
        )))
    } else {
        Err(RoCrateError::UnsupportedTerm(value.to_string()))
    }
}

fn export_metadata_descriptor(
    view: MetadataExportView<'_>,
) -> Result<MetadataDescriptor, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut conforms_to = None;
    let mut about = None;
    let mut dynamic = HashMap::new();

    for (predicate, object) in view.triples {
        let key = predicate_key(&predicate, view.ctx);
        match key.as_str() {
            "type" | "@type" => {
                if let Some(value) = object_named_node_value(&object) {
                    type_terms.push(value);
                }
            }
            "conformsTo" => conforms_to = Some(id_from_encoded_term(&object)),
            "about" => about = Some(id_from_encoded_term(&object)),
            _ => {
                let value = context_value(view.ctx, &key, &object);
                insert_entity_value(&mut dynamic, key, value);
            }
        }
    }

    Ok(MetadataDescriptor {
        id: METADATA_ID.to_string(),
        type_: data_type_from_terms(type_terms, "CreativeWork"),
        conforms_to: conforms_to.unwrap_or_else(|| Id::Id(ROCRATE_SPEC_URL.to_string())),
        about: about.unwrap_or_else(|| Id::Id(root_id(view.graph_id).to_string())),
        dynamic_entity: (!dynamic.is_empty()).then_some(dynamic),
    })
}

fn export_root_entity(view: RootExportView<'_>) -> Result<RootDataEntity, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut name = None;
    let mut description = None;
    let mut date_published = None;
    let mut license = None;
    let mut dynamic = HashMap::new();

    for (predicate, object) in view.triples {
        let key = predicate_key(&predicate, view.ctx);
        match key.as_str() {
            "type" | "@type" => {
                if let Some(value) = object_named_node_value(&object) {
                    type_terms.push(value);
                }
            }
            "name" => name = Some(literal_string(&object)?),
            "description" => description = Some(literal_string(&object)?),
            "datePublished" => date_published = Some(literal_string(&object)?),
            "license" => license = Some(license_from_encoded_term(&object)),
            "hasPart" => {}
            _ => {
                let value = context_value(view.ctx, &key, &object);
                insert_entity_value(&mut dynamic, key, value);
            }
        }
    }

    let mut ids = view.has_part;
    if !ids.is_empty() {
        dynamic.insert(
            "hasPart".to_string(),
            if ids.len() == 1 {
                EntityValue::EntityId(Id::Id(ids.remove(0)))
            } else {
                EntityValue::EntityId(Id::IdArray(ids))
            },
        );
    }

    Ok(RootDataEntity {
        id: root_id(view.graph_id).to_string(),
        type_: data_type_from_terms(type_terms, "Dataset"),
        name: name.ok_or_else(|| RoCrateError::InvalidGraph("root entity missing name".into()))?,
        description: description
            .ok_or_else(|| RoCrateError::InvalidGraph("root entity missing description".into()))?,
        date_published: date_published.ok_or_else(|| {
            RoCrateError::InvalidGraph("root entity missing datePublished".into())
        })?,
        license: license.unwrap_or_else(|| License::Id(Id::Id(ROCRATE_SPEC_URL.to_string()))),
        dynamic_entity: (!dynamic.is_empty()).then_some(dynamic),
    })
}

fn export_graph_entity(
    subject_id: &str,
    triples: Vec<(EncodedTerm, EncodedTerm)>,
    ctx: &ContextTermMap,
) -> Result<GraphVector, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut dynamic = HashMap::new();

    for (predicate, object) in triples {
        let key = predicate_key(&predicate, ctx);
        match key.as_str() {
            "type" | "@type" => {
                if let Some(value) = object_named_node_value(&object) {
                    type_terms.push(value);
                }
            }
            _ => {
                let value = context_value(ctx, &key, &object);
                insert_entity_value(&mut dynamic, key, value);
            }
        }
    }

    let data_type = data_type_from_terms(type_terms.clone(), "Thing");
    let dynamic_entity = (!dynamic.is_empty()).then_some(dynamic);
    if type_terms
        .iter()
        .any(|term| term == "Dataset" || term == "MediaObject" || term == "File")
    {
        Ok(GraphVector::DataEntity(DataEntity {
            id: subject_id.to_string(),
            type_: data_type,
            dynamic_entity,
        }))
    } else {
        Ok(GraphVector::ContextualEntity(ContextualEntity {
            id: subject_id.to_string(),
            type_: data_type,
            dynamic_entity,
        }))
    }
}

fn predicate_key(predicate: &EncodedTerm, ctx: &ContextTermMap) -> String {
    predicate
        .to_named_node()
        .map(|node| ctx.compact_predicate(node.as_str()))
        .unwrap_or_else(|| predicate.0.clone())
}

fn object_named_node_value(object: &EncodedTerm) -> Option<String> {
    object
        .to_named_node()
        .map(|node| normalize_compact_term(node.as_str()))
}

/// The bare id an entity reference denotes: an IRI unwrapped from its angle
/// brackets, a blank node kept as its `_:b0` label.
///
/// Also the emit half of the page-cursor round trip. `export_jsonld_page_after`
/// re-encodes whatever this returns with [`encoded_subject`], so the two must
/// stay inverses; a page ending on a blank-node entity used to emit no cursor at
/// all, silently truncating the caller's walk.
fn encoded_reference_value(object: &EncodedTerm) -> Option<String> {
    match object.to_term() {
        Some(Term::NamedNode(node)) => Some(node.as_str().to_string()),
        Some(Term::BlankNode(node)) => Some(format!("_:{}", node.as_str())),
        _ => None,
    }
}

fn normalize_compact_term(value: &str) -> String {
    if value == DCTERMS_CONFORMS_TO_IRI {
        return "conformsTo".to_string();
    }
    value
        .strip_prefix("http://schema.org/")
        .or_else(|| value.strip_prefix("https://schema.org/"))
        .or_else(|| value.strip_prefix("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        .or_else(|| value.strip_prefix("http://www.w3.org/2000/01/rdf-schema#"))
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn data_type_from_terms(terms: Vec<String>, default: &str) -> DataType {
    let mut terms = if terms.is_empty() {
        vec![default.to_string()]
    } else {
        terms
    };
    terms.sort();
    terms.dedup();
    if terms.len() == 1 {
        DataType::Term(terms.remove(0))
    } else {
        DataType::TermArray(terms)
    }
}

fn triples_describe_contextual_entity(triples: &[(EncodedTerm, EncodedTerm)]) -> bool {
    !triples.iter().any(|(predicate, object)| {
        predicate == &EncodedTerm::from_named_node(&vocab::rdf_type())
            && object_named_node_value(object)
                .is_some_and(|term| matches!(term.as_str(), "Dataset" | "MediaObject" | "File"))
    })
}

fn triples_have_type(triples: &[(EncodedTerm, EncodedTerm)], expected: &str) -> bool {
    triples.iter().any(|(predicate, object)| {
        predicate == &EncodedTerm::from_named_node(&vocab::rdf_type())
            && object_named_node_value(object).as_deref() == Some(expected)
    })
}

fn literal_string(term: &EncodedTerm) -> Result<String, RoCrateError> {
    match term.to_term() {
        Some(Term::Literal(literal)) => Ok(literal.value().to_string()),
        Some(other) => Err(RoCrateError::InvalidGraph(format!(
            "expected literal, found `{other}`"
        ))),
        None => Err(RoCrateError::InvalidGraph(format!(
            "failed to parse term `{}`",
            term.0
        ))),
    }
}

fn id_from_encoded_term(term: &EncodedTerm) -> Id {
    match term.to_term() {
        Some(Term::NamedNode(node)) => Id::Id(node.as_str().to_string()),
        Some(Term::BlankNode(node)) => Id::Id(format!("_:{}", node.as_str())),
        Some(Term::Literal(literal)) => Id::Id(literal.value().to_string()),
        _ => Id::Id(term.0.clone()),
    }
}

fn license_from_encoded_term(term: &EncodedTerm) -> License {
    match term.to_term() {
        Some(Term::NamedNode(node)) => License::Id(Id::Id(node.as_str().to_string())),
        Some(Term::BlankNode(node)) => License::Id(Id::Id(format!("_:{}", node.as_str()))),
        Some(Term::Literal(literal)) => License::Description(literal.value().to_string()),
        _ => License::Description(term.0.clone()),
    }
}

fn insert_entity_value(
    dynamic: &mut HashMap<String, EntityValue>,
    key: String,
    value: EntityValue,
) {
    match dynamic.remove(&key) {
        None => {
            dynamic.insert(key, value);
        }
        Some(EntityValue::EntityVec(mut values)) => {
            values.push(value);
            dynamic.insert(key, EntityValue::EntityVec(values));
        }
        Some(existing) => {
            dynamic.insert(key, EntityValue::EntityVec(vec![existing, value]));
        }
    }
}

fn entity_value_from_encoded_term(term: &EncodedTerm) -> EntityValue {
    match term.to_term() {
        Some(Term::NamedNode(node)) => EntityValue::EntityId(Id::Id(node.as_str().to_string())),
        Some(Term::BlankNode(node)) => {
            EntityValue::EntityId(Id::Id(format!("_:{}", node.as_str())))
        }
        Some(Term::Literal(literal)) => literal_entity_value(term, &literal),
        _ => EntityValue::EntityString(term.0.clone()),
    }
}

fn context_value(ctx: &ContextTermMap, key: &str, term: &EncodedTerm) -> EntityValue {
    if ctx.identifier_terms.contains(key) {
        return match term.to_term() {
            Some(Term::NamedNode(node)) => EntityValue::EntityString(
                node.as_str()
                    .strip_prefix("./")
                    .unwrap_or(node.as_str())
                    .to_string(),
            ),
            Some(Term::BlankNode(node)) => {
                EntityValue::EntityString(format!("_:{}", node.as_str()))
            }
            _ => entity_value_from_encoded_term(term),
        };
    }
    entity_value_from_encoded_term(term)
}

fn literal_entity_value(term: &EncodedTerm, literal: &oxrdf::Literal) -> EntityValue {
    let value = literal.value().to_string();
    let annotation = literal_annotation(&term.0).unwrap_or(LiteralAnnotation::Simple);
    match annotation {
        LiteralAnnotation::Simple => EntityValue::EntityString(value),
        LiteralAnnotation::Language(language) => literal_value_object(value, Some(language), None),
        LiteralAnnotation::Datatype(datatype) => match datatype.as_str() {
            XSD_STRING_IRI => EntityValue::EntityString(value),
            XSD_BOOLEAN_IRI => value
                .parse::<bool>()
                .map(EntityValue::EntityBool)
                .unwrap_or_else(|_| literal_value_object(value, None, Some(datatype))),
            XSD_INTEGER_IRI => value
                .parse::<i64>()
                .map(EntityValue::Entityi64)
                .unwrap_or_else(|_| literal_value_object(value, None, Some(datatype))),
            XSD_DOUBLE_IRI => value
                .parse::<f64>()
                .map(EntityValue::Entityf64)
                .unwrap_or_else(|_| literal_value_object(value, None, Some(datatype))),
            _ => literal_value_object(value, None, Some(datatype)),
        },
    }
}

fn literal_value_object(
    value: String,
    language: Option<String>,
    datatype: Option<String>,
) -> EntityValue {
    let mut object = HashMap::new();
    object.insert("@value".to_string(), EntityValue::EntityString(value));
    if let Some(language) = language {
        object.insert("@language".to_string(), EntityValue::EntityString(language));
    }
    if let Some(datatype) = datatype {
        object.insert("@type".to_string(), EntityValue::EntityString(datatype));
    }
    EntityValue::EntityObject(object)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiteralAnnotation {
    Simple,
    Language(String),
    Datatype(String),
}

fn literal_annotation(encoded: &str) -> Option<LiteralAnnotation> {
    let suffix = literal_suffix(encoded)?;
    if let Some(language) = suffix.strip_prefix('@') {
        Some(LiteralAnnotation::Language(language.to_string()))
    } else if let Some(datatype) = suffix.strip_prefix("^^<") {
        Some(LiteralAnnotation::Datatype(
            datatype.strip_suffix('>')?.to_string(),
        ))
    } else {
        Some(LiteralAnnotation::Simple)
    }
}

fn literal_suffix(encoded: &str) -> Option<&str> {
    let bytes = encoded.as_bytes();
    if bytes.first().copied()? != b'"' {
        return None;
    }

    let mut index = 1usize;
    while index < encoded.len() {
        match bytes[index] {
            b'"' => return encoded.get(index + 1..),
            b'\\' => {
                index += 2;
            }
            _ => {
                index += encoded[index..].chars().next()?.len_utf8();
            }
        }
    }

    None
}

fn default_context() -> RoCrateContext {
    RoCrateContext::ReferenceContext(ROCRATE_CONTEXT_URL.to_string())
}

/// Serialize an export view and replace its `@context` with the stored raw
/// context JSON. Used when a graph has a custom context that `RoCrateContext`
/// cannot represent (e.g. complex term definitions).
fn splice_context_json(
    mut document: serde_json::Value,
    raw_context: &str,
    pretty: bool,
) -> Result<String, RoCrateError> {
    let context: serde_json::Value = serde_json::from_str(raw_context)?;
    if let Some(object) = document.as_object_mut() {
        object.insert("@context".to_string(), context);
    }
    if pretty {
        Ok(serde_json::to_string_pretty(&document)?)
    } else {
        Ok(serde_json::to_string(&document)?)
    }
}

/// Simple `term -> IRI` mappings and their reverse, derived from a stored
/// RO-Crate `@context`. String mappings from inline embedded context objects
/// are captured, and object definitions carrying a string `@id` are expanded to
/// that IRI; other complex shapes are skipped. Duplicate terms resolve
/// last-write-wins (a later entry overrides an earlier one), with a warning on
/// the import path (see [`collect_context_terms`]).
#[derive(Debug, Default)]
struct ContextTermMap {
    forward: HashMap<String, String>,
    reverse: HashMap<String, String>,
    identifier_terms: HashSet<String>,
}

impl ContextTermMap {
    fn from_raw(raw: Option<&str>) -> Self {
        let mut forward = HashMap::new();
        let mut identifier_terms = HashSet::new();
        if let Some(raw) = raw {
            match serde_json::from_str::<serde_json::Value>(raw) {
                // Export path: the context was already validated and warned about
                // at import time, so collect quietly (see `collect_context_terms`).
                Ok(value) => {
                    collect_context_terms(&value, &mut forward, false);
                    collect_identifier_terms(&value, &mut identifier_terms);
                }
                Err(error) => tracing::debug!(
                    %error,
                    "stored RO-Crate @context is not valid JSON; skipping term compaction"
                ),
            }
        }
        Self::from_forward(forward, identifier_terms)
    }

    fn from_forward(forward: HashMap<String, String>, identifier_terms: HashSet<String>) -> Self {
        let mut reverse: HashMap<String, String> = HashMap::new();
        for (term, iri) in &forward {
            match reverse.get(iri) {
                // Deterministic reverse mapping: keep the lexicographically
                // smallest term when several terms alias the same IRI.
                Some(existing) if existing.as_str() <= term.as_str() => {}
                _ => {
                    reverse.insert(iri.clone(), term.clone());
                }
            }
        }
        Self {
            forward,
            reverse,
            identifier_terms,
        }
    }

    /// Compact a predicate IRI back to a context term.
    ///
    /// A custom mapping wins over the built-in schema.org/rdf/rdfs compaction.
    /// The built-in compaction is suppressed when the stored context redefines
    /// the resulting term to a different IRI, so an emitted compact key always
    /// resolves back to exactly the stored predicate IRI.
    fn compact_predicate(&self, iri: &str) -> String {
        if let Some(term) = self.reverse.get(iri) {
            return term.clone();
        }
        let builtin = normalize_compact_term(iri);
        if builtin != iri
            && let Some(mapped) = self.forward.get(&builtin)
            && mapped != iri
        {
            return iri.to_string();
        }
        builtin
    }
}

fn collect_identifier_terms(context: &serde_json::Value, terms: &mut HashSet<String>) {
    match context {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_identifier_terms(item, terms);
            }
        }
        serde_json::Value::Object(entries) => {
            for (term, definition) in entries {
                if definition
                    .as_object()
                    .and_then(|definition| definition.get("@type"))
                    .and_then(serde_json::Value::as_str)
                    == Some("@id")
                {
                    terms.insert(term.clone());
                }
            }
        }
        _ => {}
    }
}

/// Whether a submitted `@context` is equivalent to the bare default RO-Crate
/// context (a plain reference string, or a single-element array of it).
fn is_bare_default_context(context: &serde_json::Value) -> bool {
    match context {
        serde_json::Value::String(url) => url == ROCRATE_CONTEXT_URL,
        serde_json::Value::Array(items) => {
            items.len() == 1
                && items
                    .first()
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|url| url == ROCRATE_CONTEXT_URL)
        }
        _ => false,
    }
}

/// Serialize the submitted `@context` verbatim for storage, or `None` when it is
/// absent, degenerate, or equivalent to the bare default RO-Crate context.
///
/// Only strings, arrays, and objects can carry a usable JSON-LD context.
/// Degenerate values (`null`, numbers, booleans) carry no mappings and are
/// treated as "no custom context" so export falls back to the bare default URL
/// rather than round-tripping a nonsensical `@context`.
fn extract_raw_context(value: &serde_json::Value) -> Option<String> {
    let context = value.as_object()?.get("@context")?;
    if !matches!(
        context,
        serde_json::Value::String(_) | serde_json::Value::Array(_) | serde_json::Value::Object(_)
    ) {
        return None;
    }
    if is_bare_default_context(context) {
        return None;
    }
    match serde_json::to_string(context) {
        Ok(raw) => Some(raw),
        Err(error) => {
            tracing::warn!(%error, "failed to serialize submitted @context; treating as default");
            None
        }
    }
}

fn extract_raw_license(value: &serde_json::Value) -> Option<String> {
    let document = value.as_object()?;
    let mut terms = HashMap::new();
    if let Some(context) = document.get("@context") {
        collect_context_terms(context, &mut terms, false);
    }
    let entries = document.iter().find_map(|(key, value)| {
        (key == "@graph" || key == "graph" || terms.get(key).is_some_and(|iri| iri == "@graph"))
            .then(|| value.as_array())
            .flatten()
    })?;
    let root_id = entries.iter().find_map(|entry| {
        let entity = entry.as_object()?;
        (normalize_entity_id(submitted_id(entity, &terms)?) == METADATA_ID)
            .then(|| {
                entity.iter().find_map(|(key, value)| {
                    (submitted_predicate(key, &terms).as_deref()
                        == Some(vocab::schema_about().as_str()))
                    .then(|| reference_id(value, &terms))
                    .flatten()
                })
            })
            .flatten()
    })?;
    let root = entries.iter().find_map(|entry| {
        let entity = entry.as_object()?;
        (normalize_entity_id(submitted_id(entity, &terms)?) == root_id).then_some(entity)
    })?;
    let license = root.iter().find_map(|(key, value)| {
        (submitted_predicate(key, &terms).as_deref() == Some(vocab::schema_license().as_str()))
            .then_some(value)
    })?;
    serde_json::to_string(license).ok()
}

/// Insert a term mapping, warning (import path only) when it remaps an
/// already-collected term to a different IRI. Last definition wins.
fn insert_context_term(map: &mut HashMap<String, String>, term: &str, iri: String, warn: bool) {
    if warn
        && let Some(previous) = map.get(term)
        && *previous != iri
    {
        tracing::warn!(
            term = %term,
            previous = %previous,
            replacement = %iri,
            "duplicate @context term remapped to a different IRI (last definition wins)"
        );
    }
    map.insert(term.to_string(), iri);
}

/// Collect `term -> IRI` mappings from a `@context`.
///
/// Array entries are processed in order so later definitions override earlier
/// ones (last-write-wins). String term definitions map directly, and object
/// term definitions carrying a string `@id` are expanded to that IRI. Reference
/// URLs other than the RO-Crate base, `@`-keywords, and object definitions
/// without a string `@id` cannot be expanded here and are skipped. When `warn`
/// is set (import path) each skip — and each duplicate term that remaps to a
/// different IRI — is logged at `warn` level; the export path passes `false` so
/// it does not re-log on every export.
fn collect_context_terms(
    context: &serde_json::Value,
    map: &mut HashMap<String, String>,
    warn: bool,
) {
    match context {
        serde_json::Value::String(url) => {
            if url != ROCRATE_CONTEXT_URL && warn {
                tracing::warn!(
                    context = %url,
                    "ignoring non-RO-Crate reference @context for term expansion"
                );
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_context_terms(item, map, warn);
            }
        }
        serde_json::Value::Object(entries) => {
            for (term, definition) in entries {
                if term.starts_with('@') {
                    if warn {
                        tracing::warn!(
                            key = %term,
                            "ignoring @context keyword entry for term expansion"
                        );
                    }
                    continue;
                }
                match definition {
                    serde_json::Value::String(iri) => {
                        insert_context_term(map, term, iri.clone(), warn);
                    }
                    serde_json::Value::Object(definition_object) => {
                        match definition_object
                            .get("@id")
                            .and_then(serde_json::Value::as_str)
                        {
                            Some(iri) => insert_context_term(map, term, iri.to_string(), warn),
                            None if warn => tracing::warn!(
                                term = %term,
                                "ignoring complex @context term definition without a string @id for term expansion"
                            ),
                            None => {}
                        }
                    }
                    _ if warn => tracing::warn!(
                        term = %term,
                        "ignoring complex @context term definition for term expansion"
                    ),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn rocrate_triples(rocrate: &RoCrate) -> Result<BTreeSet<TripleKey>, RoCrateError> {
    let rdf_graph = rocrate_to_rdf_with_options(
        rocrate,
        ContextResolverBuilder::default(),
        ConversionOptions::AllowRelative,
    )?;

    let mut triples = BTreeSet::new();
    for triple in rdf_graph {
        triples.insert(triple_key_from_rdf(&triple));
    }
    Ok(triples)
}

fn triple_key_from_rdf(triple: &Triple) -> TripleKey {
    (
        EncodedTerm::from(&triple.subject),
        EncodedTerm::from_named_node(&triple.predicate),
        EncodedTerm::from_term(&triple.object),
    )
}

fn normalize_rocrate(rocrate: &mut RoCrate) {
    for entry in &mut rocrate.graph {
        if let GraphVector::MetadataDescriptor(metadata) = entry {
            normalize_metadata_descriptor(metadata);
        }
    }
}

fn normalize_metadata_descriptor(metadata: &mut MetadataDescriptor) {
    if let Some(dynamic) = &mut metadata.dynamic_entity
        && let Some(value) = dynamic
            .remove("conformsTo")
            .or_else(|| dynamic.remove("schema:conformsTo"))
            .or_else(|| dynamic.remove("http://schema.org/conformsTo"))
            .or_else(|| dynamic.remove("https://schema.org/conformsTo"))
            .or_else(|| dynamic.remove(DCTERMS_CONFORMS_TO_IRI))
        && let Some(id) = first_identifier(&value)
    {
        metadata.conforms_to = Id::Id(id);
    }

    if let Id::Id(id) = &metadata.conforms_to
        && id == ROCRATE_CONTEXT_URL
    {
        metadata.conforms_to = Id::Id(ROCRATE_SPEC_URL.to_string());
    }
}

fn first_identifier(value: &EntityValue) -> Option<String> {
    match value {
        EntityValue::EntityId(Id::Id(id)) => Some(id.clone()),
        EntityValue::EntityId(Id::IdArray(ids)) => preferred_identifier(ids),
        EntityValue::EntityVec(values) => values.iter().find_map(first_identifier),
        _ => None,
    }
}

fn preferred_identifier(ids: &[String]) -> Option<String> {
    ids.iter()
        .find(|id| id.as_str() != ROCRATE_CONTEXT_URL)
        .cloned()
        .or_else(|| ids.first().cloned())
}

fn normalize_property(property: &str) -> String {
    property
        .strip_prefix("schema:")
        .or_else(|| property.strip_prefix("http://schema.org/"))
        .or_else(|| property.strip_prefix("https://schema.org/"))
        .map(str::to_string)
        .unwrap_or_else(|| property.to_string())
}

fn normalize_term(term: &str) -> String {
    normalize_property(term)
}

fn normalize_entity_id(id: &str) -> String {
    if id == METADATA_ID
        || id.starts_with("./")
        || id.starts_with("../")
        || id.starts_with('#')
        || id.starts_with("_:")
        || id.contains("://")
        || (id.contains(':') && !id.contains('/'))
    {
        id.to_string()
    } else {
        format!("./{id}")
    }
}

fn license_from_str(license: &str) -> Result<License, RoCrateError> {
    if looks_like_identifier(license) {
        Ok(License::Id(Id::Id(
            encoded_reference_term(license)?
                .to_named_node()
                .map(|node| node.as_str().to_string())
                .unwrap_or_else(|| license.to_string()),
        )))
    } else {
        Ok(License::Description(license.to_string()))
    }
}

fn looks_like_identifier(value: &str) -> bool {
    value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || value.contains("://")
        || (value.contains(':') && !value.contains(' '))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::sync::{
        CraqleGraphEvent, CraqleGraphSync, CraqleSyncError, SyncResult, TopicCatchup,
    };
    use irokle::reducer::EventRecord;

    /// A [`CraqleGraphSync`] decorator that delegates every call to `inner`,
    /// except the two publishes an import performs, each of which fails while
    /// its flag is set. `fail_changes` injects a phase-1 (quad) failure, which
    /// under publish-first must leave no local trace at all; `fail_context`
    /// injects a phase-2 (context) failure after phase 1 has already committed
    /// and published.
    struct FlakySync {
        inner: Arc<dyn CraqleGraphSync>,
        fail_changes: AtomicBool,
        fail_context: AtomicBool,
    }

    impl FlakySync {
        fn new(inner: Arc<dyn CraqleGraphSync>) -> Self {
            Self {
                inner,
                fail_changes: AtomicBool::new(false),
                fail_context: AtomicBool::new(false),
            }
        }
    }

    impl CraqleGraphSync for FlakySync {
        fn publish_changes(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            changes: Vec<MaterializedQuadChange>,
        ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
            if self.fail_changes.load(Ordering::SeqCst) {
                return Err(CraqleSyncError::InvalidEvent(
                    "injected quad publish failure".to_string(),
                ));
            }
            self.inner.publish_changes(store, graph, changes)
        }

        fn publish_policy(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            policy: crate::core::GraphPolicy,
        ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
            self.inner.publish_policy(store, graph, policy)
        }

        fn publish_delete(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
        ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
            self.inner.publish_delete(store, graph)
        }

        fn publish_context(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            context: Option<String>,
            license: Option<String>,
            license_digest: Option<[u8; 32]>,
            tag: crate::core::ContextTag,
        ) -> SyncResult<EventRecord<CraqleGraphEvent>> {
            if self.fail_context.load(Ordering::SeqCst) {
                return Err(CraqleSyncError::InvalidEvent(
                    "injected context publish failure".to_string(),
                ));
            }
            self.inner
                .publish_context(store, graph, context, license, license_digest, tag)
        }

        fn graph_topic_id(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
        ) -> SyncResult<Option<irokle::TopicId>> {
            self.inner.graph_topic_id(store, graph)
        }

        fn ensure_graph_topic(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
        ) -> SyncResult<irokle::TopicId> {
            self.inner.ensure_graph_topic(store, graph)
        }

        fn bind_graph_topic(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            topic_id: irokle::TopicId,
        ) -> SyncResult<()> {
            self.inner.bind_graph_topic(store, graph, topic_id)
        }

        fn bind_graph_topic_if_present(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
        ) -> SyncResult<Option<irokle::TopicId>> {
            self.inner.bind_graph_topic_if_present(store, graph)
        }

        fn mint_graph_topic(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            initial_peers: std::collections::BTreeSet<irokle::PeerId>,
        ) -> SyncResult<irokle::TopicId> {
            self.inner.mint_graph_topic(store, graph, initial_peers)
        }

        fn craqle_topic_ids(&self) -> SyncResult<Vec<irokle::TopicId>> {
            self.inner.craqle_topic_ids()
        }

        fn topic_records_since(
            &self,
            topic_id: irokle::TopicId,
            cursor: Option<&[u8]>,
        ) -> SyncResult<TopicCatchup> {
            self.inner.topic_records_since(topic_id, cursor)
        }

        fn add_peer(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            peer: irokle::PeerId,
        ) -> SyncResult<()> {
            self.inner.add_peer(store, graph, peer)
        }

        fn remove_peer(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
            peer: irokle::PeerId,
        ) -> SyncResult<()> {
            self.inner.remove_peer(store, graph, peer)
        }

        fn sync_status(
            &self,
            store: &crate::store::GraphStore,
            graph: &GraphId,
        ) -> SyncResult<Vec<irokle::SyncPeerStatus>> {
            self.inner.sync_status(store, graph)
        }
    }

    /// A store plus a manager whose sync layer can be made to fail either
    /// publish phase on demand.
    fn flaky_manager() -> (
        tempfile::TempDir,
        Arc<crate::store::GraphStore>,
        Arc<FlakySync>,
        RoCrateManager,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::GraphStore::open(dir.path()).unwrap());
        let search = Arc::new(crate::search::SearchIndex::open_in_memory().unwrap());
        let sparql = Arc::new(crate::sparql::SparqlEngine::new(store.clone(), search));

        let node = irokle::Irokle::builder().build().unwrap();
        let inner: Arc<dyn CraqleGraphSync> = Arc::new(crate::sync::IrokleGraphSync::new(
            node,
            crate::sync::CraqleIrokleOptions::new(),
        ));
        let flaky = Arc::new(FlakySync::new(inner));
        let sync: Arc<dyn CraqleGraphSync> = flaky.clone();

        let engine = Arc::new(ReplicationEngine::new_with_sync(
            store.clone(),
            sparql,
            crate::core::ActorId::random(),
            Some(sync),
        ));
        (dir, store.clone(), flaky, RoCrateManager::new(engine))
    }

    /// Everything a write is allowed to move, read in one shot so a failed
    /// publish can be compared against the state that preceded it.
    #[derive(Debug, PartialEq)]
    struct LocalState {
        fingerprint: (u64, [u8; 32], [u8; 32]),
        clock: crate::core::VectorClock,
        diagnostics: crate::core::GraphDiagnostics,
        context: Option<String>,
        context_tag: crate::core::ContextTag,
    }

    impl LocalState {
        fn read(store: &crate::store::GraphStore, graph: &GraphId) -> Self {
            Self {
                fingerprint: store.graph_fingerprint(graph).unwrap(),
                clock: store.get_vector_clock(graph).unwrap(),
                diagnostics: store.graph_diagnostics(graph).unwrap(),
                context: store.graph_context(graph).unwrap(),
                context_tag: store.graph_context_tag(graph).unwrap(),
            }
        }
    }

    /// G4 publish-first for the *quad* phase: a `publish_changes` that fails
    /// must move no local state whatsoever.
    ///
    /// The context phase already had fault injection; this is the phase that
    /// matters more, because applying before publishing would leave this node
    /// holding quads and a clock entry no peer will ever be told about — a
    /// divergence nothing later reconciles. Every field a write can touch is
    /// compared, not just the quad count.
    #[test]
    fn publish_persists_nothing() {
        let (_dir, store, flaky, manager) = flaky_manager();
        let graph = GraphId::new("urn:test:quad-publish-failure");
        let document = |extra: &str| {
            serde_json::json!({
                "@context": ROCRATE_CONTEXT_URL,
                "@graph": [
                    {
                        "@id": METADATA_ID,
                        "@type": "CreativeWork",
                        "conformsTo": {"@id": ROCRATE_SPEC_URL},
                        "about": {"@id": graph.as_str()}
                    },
                    {
                        "@id": graph.as_str(),
                        "@type": "Dataset",
                        "name": "Publish First Crate",
                        "description": extra,
                        "datePublished": "2025-01-01",
                        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
                    }
                ]
            })
            .to_string()
        };

        manager
            .import_jsonld(graph.clone(), &document("baseline"))
            .unwrap();
        let before = LocalState::read(&store, &graph);
        assert!(before.fingerprint.0 > 0, "the baseline import must land");

        flaky.fail_changes.store(true, Ordering::SeqCst);
        let failed = manager.import_jsonld(graph.clone(), &document("amended"));
        assert!(
            failed.is_err(),
            "the import must surface the injected quad publish failure"
        );
        assert_eq!(
            before,
            LocalState::read(&store, &graph),
            "a failed quad publish moved local state"
        );

        // And the failure is not terminal: retrying once the fault clears lands.
        flaky.fail_changes.store(false, Ordering::SeqCst);
        manager
            .import_jsonld(graph.clone(), &document("amended"))
            .unwrap();
        assert_ne!(
            before,
            LocalState::read(&store, &graph),
            "the retried import must apply"
        );
    }

    /// The same guarantee on a graph that does not exist yet: a failed quad
    /// publish must not leave a half-created graph behind.
    #[test]
    fn publish_creates_nothing() {
        let (_dir, store, flaky, manager) = flaky_manager();
        let graph = GraphId::new("urn:test:quad-publish-failure-fresh");
        flaky.fail_changes.store(true, Ordering::SeqCst);

        let document = serde_json::json!({
            "@context": ROCRATE_CONTEXT_URL,
            "@graph": [
                {
                    "@id": METADATA_ID,
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": ROCRATE_SPEC_URL},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Never Published",
                    "description": "nothing may survive this",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
                }
            ]
        });

        assert!(
            manager
                .import_jsonld(graph.clone(), &document.to_string())
                .is_err()
        );
        assert!(
            store.graph_is_empty(&graph).unwrap(),
            "a failed quad publish must leave no quads"
        );
        assert!(
            store.get_vector_clock(&graph).unwrap().0.is_empty(),
            "a failed quad publish must not advance the clock"
        );
        assert_eq!(
            crate::core::ContextTag::GENESIS,
            store.graph_context_tag(&graph).unwrap(),
            "phase 2 must never run when phase 1 failed"
        );
    }

    /// A failed context publish (phase 2) leaves the already-applied quads
    /// (phase 1) in place with the stored context unchanged, and re-importing the
    /// same document heals it: the empty quad diff is a no-op while the context
    /// store/publish is retried.
    #[test]
    fn context_publish_failure_leaves_quads_and_heals_on_reimport() {
        let (_dir, store, flaky, manager) = flaky_manager();
        flaky.fail_context.store(true, Ordering::SeqCst);

        let graph = GraphId::new("urn:test:context-publish-failure");
        let organism_iri = "https://w3id.org/aruna/profiles/proteomics#organism";
        let document = serde_json::json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                { "organism": organism_iri }
            ],
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Failure Mode Crate",
                    "description": "Context publish fails then heals",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                    "organism": "Homo sapiens"
                }
            ]
        });

        // Phase 1 (quads) succeeds; phase 2 (context publish) is forced to fail.
        let first = manager.import_jsonld(graph.clone(), &document.to_string());
        assert!(
            first.is_err(),
            "import should surface the injected context publish failure"
        );

        // The quads were applied (phase 1 committed and published) ...
        assert!(
            !store.graph_is_empty(&graph).unwrap(),
            "quads should remain after the context publish failure"
        );
        assert!(
            manager
                .current_triples(&manager.crate_ctx(&graph).unwrap())
                .unwrap()
                .iter()
                .any(|(_, predicate, _)| predicate
                    .to_named_node()
                    .is_some_and(|node| node.as_str() == organism_iri)),
            "the custom-profile organism predicate should be present"
        );
        // ... but the stored context register is unchanged (publish-first).
        assert_eq!(store.graph_context(&graph).unwrap(), None);
        assert_eq!(
            store.graph_context_tag(&graph).unwrap(),
            crate::core::ContextTag::GENESIS
        );

        // Clear the fault and re-import the SAME document. The quad diff is empty
        // (a no-op batch), and because the stored context still differs, the
        // `current == context` guard does not trip, so the context is retried.
        flaky.fail_context.store(false, Ordering::SeqCst);
        let second = manager.import_jsonld(graph.clone(), &document.to_string());
        assert!(second.is_ok(), "re-import should heal: {second:?}");

        let stored = store.graph_context(&graph).unwrap();
        assert!(
            stored
                .as_deref()
                .is_some_and(|context| context.contains(organism_iri)),
            "healed context should carry the profile IRI: {stored:?}"
        );
        // Exactly one: the failed publish persisted nothing, so the heal mints
        // `next_local(GENESIS)`. `>= 1` would also accept a double-mint, which is
        // the bug G5's publish-first ordering exists to prevent.
        let healed_tag = store.graph_context_tag(&graph).unwrap();
        assert_eq!(
            1, healed_tag.counter,
            "the heal must mint exactly one tag past genesis"
        );

        // Healing converges rather than oscillating: a third import of the same
        // document now finds `current == context`, so it neither republishes nor
        // mints a new tag. The LWW register (G5) has reached a fixpoint.
        manager
            .import_jsonld(graph.clone(), &document.to_string())
            .unwrap();
        assert_eq!(store.graph_context(&graph).unwrap(), stored);
        assert_eq!(
            store.graph_context_tag(&graph).unwrap(),
            healed_tag,
            "an unchanged re-import must not mint a new context tag"
        );
    }

    #[test]
    fn profile_summary_includes_only_resource_descriptor_artifact_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::GraphStore::open(dir.path()).unwrap());
        let search = Arc::new(crate::search::SearchIndex::open_in_memory().unwrap());
        let sparql = Arc::new(crate::sparql::SparqlEngine::new(store.clone(), search));
        let engine = Arc::new(ReplicationEngine::new(
            store,
            sparql,
            crate::core::ActorId::random(),
        ));
        let manager = RoCrateManager::new(engine);
        let graph = GraphId::new("urn:test:profile-summary-artifacts");
        let document = serde_json::json!({
            "@context": [
                ROCRATE_CONTEXT_URL,
                {
                    "hasResource": "http://www.w3.org/ns/dx/prof#hasResource",
                    "hasArtifact": PROF_HAS_ARTIFACT_IRI,
                    "text": "http://schema.org/text"
                }
            ],
            "@graph": [
                {
                    "@id": METADATA_ID,
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": ROCRATE_SPEC_URL},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Profile Crate",
                    "description": "Profile rules and an ordinary data file",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                    "conformsTo": {"@id": "#profile"},
                    // Profile artifacts are data entities, so RO-Crate 1.2
                    // requires the root to link them by `hasPart` as well.
                    "hasPart": [
                        {"@id": "./data/plain.txt"},
                        {"@id": "./mode.json"},
                        {"@id": "./schema.json"}
                    ]
                },
                {
                    "@id": "#profile",
                    "@type": "http://www.w3.org/ns/dx/prof#Profile",
                    "name": "Test Profile",
                    "hasResource": [
                        {"@id": "#mode-descriptor"},
                        {"@id": "#schema-descriptor"}
                    ]
                },
                {
                    "@id": "#mode-descriptor",
                    "@type": PROF_RESOURCE_DESCRIPTOR_IRI,
                    "name": "Mode Rules",
                    "hasArtifact": {"@id": "./mode.json"}
                },
                {
                    "@id": "#schema-descriptor",
                    "@type": PROF_RESOURCE_DESCRIPTOR_IRI,
                    "name": "Schema Rules",
                    "hasArtifact": {"@id": "./schema.json"}
                },
                {
                    "@id": "./mode.json",
                    "@type": "File",
                    "name": "Mode Rules",
                    "text": "{\"mode\":\"strict\"}"
                },
                {
                    "@id": "./schema.json",
                    "@type": "File",
                    "name": "Schema Rules",
                    "text": "{\"required\":[\"name\"]}"
                },
                {
                    "@id": "./data/plain.txt",
                    "@type": "File",
                    "name": "Plain Data",
                    "text": "ordinary data content"
                }
            ]
        });

        manager
            .import_jsonld(graph.clone(), &document.to_string())
            .unwrap();
        let summary_json = manager.export_jsonld_summary(&graph).unwrap();
        let summary: serde_json::Value = serde_json::from_str(&summary_json).unwrap();
        let entries = summary["@graph"].as_array().unwrap();
        let mode = entries
            .iter()
            .find(|entity| entity["@id"].as_str() == Some("./mode.json"))
            .unwrap();
        let schema = entries
            .iter()
            .find(|entity| entity["@id"].as_str() == Some("./schema.json"))
            .unwrap();
        assert_eq!(mode["text"], serde_json::json!("{\"mode\":\"strict\"}"));
        assert_eq!(
            schema["text"],
            serde_json::json!("{\"required\":[\"name\"]}")
        );
        assert!(
            entries
                .iter()
                .all(|entity| entity["@id"].as_str() != Some("./data/plain.txt")),
            "ordinary root hasPart files must stay out of summary exports"
        );

        let roundtrip_graph = GraphId::new("urn:test:profile-summary-artifacts-roundtrip");
        manager
            .import_jsonld(roundtrip_graph.clone(), &summary_json)
            .unwrap();
        let roundtrip: serde_json::Value =
            serde_json::from_str(&manager.export_jsonld_summary(&roundtrip_graph).unwrap())
                .unwrap();
        let roundtrip_entries = roundtrip["@graph"].as_array().unwrap();
        assert!(roundtrip_entries.iter().any(|entity| {
            entity["@id"].as_str() == Some("./mode.json")
                && entity["text"] == serde_json::json!("{\"mode\":\"strict\"}")
        }));
        assert!(roundtrip_entries.iter().any(|entity| {
            entity["@id"].as_str() == Some("./schema.json")
                && entity["text"] == serde_json::json!("{\"required\":[\"name\"]}")
        }));
    }
}
