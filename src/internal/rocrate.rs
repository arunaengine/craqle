use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::core::{Batch, EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use crate::replication::ReplicationEngine;
use oxrdf::{NamedNode, Term, Triple};
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

const ROCRATE_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.2/context";
const ROCRATE_SPEC_URL: &str = "https://w3id.org/ro/crate/1.2";
const XSD_BOOLEAN_IRI: &str = "http://www.w3.org/2001/XMLSchema#boolean";
const XSD_DOUBLE_IRI: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_INTEGER_IRI: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_STRING_IRI: &str = "http://www.w3.org/2001/XMLSchema#string";
const ROOT_ID: &str = "./";
const METADATA_ID: &str = "ro-crate-metadata.json";
type TripleKey = (EncodedTerm, EncodedTerm, EncodedTerm);

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
pub struct RoCrateManager {
    engine: Arc<ReplicationEngine>,
}

impl RoCrateManager {
    pub fn new(engine: Arc<ReplicationEngine>) -> Self {
        Self { engine }
    }

    /// Create a new RO-Crate with all required base entities.
    pub fn create_crate(
        &self,
        graph_id: GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: &str,
    ) -> Result<Batch, RoCrateError> {
        if self.graph_is_empty(&graph_id)? {
            let changes = vec![
                insert_change(
                    &graph_id,
                    METADATA_ID,
                    &vocab::rdf_type(),
                    EncodedTerm::from_named_node(&vocab::schema_creative_work()),
                ),
                insert_change(
                    &graph_id,
                    METADATA_ID,
                    &vocab::schema_conforms_to(),
                    encoded_identifier(ROCRATE_SPEC_URL),
                ),
                insert_change(
                    &graph_id,
                    METADATA_ID,
                    &vocab::schema_about(),
                    encoded_identifier(ROOT_ID),
                ),
                insert_change(
                    &graph_id,
                    ROOT_ID,
                    &vocab::rdf_type(),
                    EncodedTerm::from_named_node(&vocab::schema_dataset()),
                ),
                insert_change(
                    &graph_id,
                    ROOT_ID,
                    &vocab::schema_name(),
                    encoded_literal(name),
                ),
                insert_change(
                    &graph_id,
                    ROOT_ID,
                    &vocab::schema_description(),
                    encoded_literal(description),
                ),
                insert_change(
                    &graph_id,
                    ROOT_ID,
                    &vocab::schema_date_published(),
                    encoded_literal(date_published),
                ),
                insert_change(
                    &graph_id,
                    ROOT_ID,
                    &vocab::schema_license(),
                    encoded_license_value(license)?,
                ),
            ];
            return Ok(self.engine.local_apply_changes(&graph_id, changes)?);
        }

        let rocrate = RoCrate {
            context: default_context(),
            graph: vec![
                GraphVector::MetadataDescriptor(MetadataDescriptor {
                    id: METADATA_ID.to_string(),
                    type_: DataType::Term("CreativeWork".to_string()),
                    conforms_to: Id::Id(ROCRATE_SPEC_URL.to_string()),
                    about: Id::Id(ROOT_ID.to_string()),
                    dynamic_entity: Some(HashMap::new()),
                }),
                GraphVector::RootDataEntity(RootDataEntity {
                    id: ROOT_ID.to_string(),
                    type_: DataType::Term("Dataset".to_string()),
                    name: name.to_string(),
                    description: description.to_string(),
                    date_published: date_published.to_string(),
                    license: license_from_str(license)?,
                    dynamic_entity: Some(HashMap::new()),
                }),
            ],
        };

        self.replace_graph_with_rocrate(&graph_id, rocrate)
    }

    /// Add a data entity with automatic hasPart linkage from root.
    pub fn add_data_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        let entity_id = normalize_entity_id(entity_id);
        self.require_rocrate_initialized(graph_id)?;
        self.upsert_data_entity_incremental(
            graph_id,
            ROOT_ID,
            &entity_id,
            entity_type,
            name,
            additional_triples,
        )
    }

    pub fn append_new_root_data_entities(
        &self,
        graph_id: &GraphId,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport, RoCrateError> {
        self.append_new_data_entities_under(graph_id, ROOT_ID, entities)
    }

    pub fn append_new_data_entities_under(
        &self,
        graph_id: &GraphId,
        parent_id: &str,
        entities: Vec<NewDataEntity>,
    ) -> Result<AppendDataEntitiesReport, RoCrateError> {
        self.require_rocrate_initialized(graph_id)?;

        let parent_id = normalize_entity_id(parent_id);
        if !self.visible_subject_exists(graph_id, &parent_id)? {
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
                encoded_identifier(&entity_id),
            ));

            for (predicate, object) in entity_subject_triples(
                &entity_id,
                &entity.entity_type,
                &entity.name,
                &entity.additional_triples,
            )? {
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph_id.clone(),
                    subject: EncodedTerm::from_named_node(&NamedNode::new_unchecked(&entity_id)),
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
        Ok(AppendDataEntitiesReport {
            batch,
            entity_count,
            change_count,
        })
    }

    /// Add a contextual entity (no hasPart linkage needed).
    pub fn add_contextual_entity(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        self.require_rocrate_initialized(graph_id)?;
        let entity_id = normalize_entity_id(entity_id);
        let mut changes = self.replace_subject_changes(
            graph_id,
            &entity_id,
            entity_subject_triples(&entity_id, entity_type, name, &additional_triples)?,
        )?;
        Ok(self
            .engine
            .local_apply_changes(graph_id, std::mem::take(&mut changes))?)
    }

    /// Export a graph to RO-Crate JSON-LD.
    pub fn export_jsonld(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        Ok(serde_json::to_string_pretty(
            &self.current_rocrate(graph_id)?,
        )?)
    }

    /// Export a lightweight partial RO-Crate view without data entities.
    pub fn export_jsonld_summary(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        Ok(serde_json::to_string(
            &self.build_partial_export_view(graph_id, &[])?,
        )?)
    }

    /// Export an offset-based partial RO-Crate page of root-linked data entities.
    pub fn export_jsonld_page(
        &self,
        graph_id: &GraphId,
        offset: usize,
        limit: usize,
    ) -> Result<RoCratePage, RoCrateError> {
        let (total, page) = self.root_linked_data_entity_page(graph_id, offset, limit)?;
        let jsonld = serde_json::to_string(&self.build_partial_export_view(graph_id, &page)?)?;
        let returned = page.len();
        let has_more = offset + returned < total;
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_named_node_value))
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
    pub fn export_jsonld_page_after(
        &self,
        graph_id: &GraphId,
        after_entity_id: Option<&str>,
        limit: usize,
    ) -> Result<RoCratePage, RoCrateError> {
        let (total, mut page) = self.root_linked_data_entity_page_after(
            graph_id,
            after_entity_id,
            limit.saturating_add(1),
        )?;
        page.truncate(limit.saturating_add(1));
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        let returned = page.len();
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_named_node_value))
            .flatten();
        let jsonld = serde_json::to_string(&self.build_partial_export_view(graph_id, &page)?)?;

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
    pub fn import_jsonld(&self, graph_id: GraphId, jsonld: &str) -> Result<Batch, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        if self.graph_is_missing_or_empty(&graph_id)? {
            return self.import_jsonld_into_empty_graph_trusted(graph_id, value);
        }

        self.replace_jsonld_in_existing_graph(graph_id, value)
    }

    /// Strict import path that validates complete RO-Crate semantics even for
    /// new-graph bootstrap imports.
    pub fn import_jsonld_checked(
        &self,
        graph_id: GraphId,
        jsonld: &str,
    ) -> Result<Batch, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        if self.graph_is_missing_or_empty(&graph_id)? {
            return self.import_jsonld_into_empty_graph(graph_id, value);
        }

        self.replace_jsonld_in_existing_graph(graph_id, value)
    }

    /// Fast path for trusted bootstrap imports into a new or empty graph.
    ///
    /// This skips semantic RO-Crate validation and current-state diffing, and
    /// is intended for callers that already trust the input document.
    pub fn bootstrap_jsonld_trusted(
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
        self.import_jsonld_into_empty_graph_trusted(graph_id, value)
    }

    /// Compute the canonical change set for replacing a graph with a JSON-LD RO-Crate.
    pub fn plan_import_jsonld(
        &self,
        graph_id: &GraphId,
        jsonld: &str,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let value: serde_json::Value = serde_json::from_str(jsonld)?;
        self.plan_import_value(graph_id, value)
    }

    /// Update a property on an entity.
    ///
    /// When `old_value` is `Some(v)`, only the triple matching `v` is replaced.
    /// When `old_value` is `None`, **all** existing values for the given predicate
    /// are removed before inserting `new_value` (replace-all semantics).
    pub fn update_property(
        &self,
        graph_id: &GraphId,
        entity_id: &str,
        predicate: &str,
        old_value: Option<&str>,
        new_value: &str,
    ) -> Result<Batch, RoCrateError> {
        self.require_rocrate_initialized(graph_id)?;
        let entity_id = normalize_entity_id(entity_id);
        let subject = EncodedTerm::from_named_node(&NamedNode::new_unchecked(&entity_id));
        let current = self.subject_triples(graph_id, &entity_id)?;
        if current.is_empty() {
            return Err(RoCrateError::EntityNotFound(entity_id));
        }

        let property = normalize_property(predicate);
        let predicate_node = property_named_node(&property)?;
        let predicate_term = EncodedTerm::from_named_node(&predicate_node);
        let mut changes = Vec::new();

        match old_value {
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
            object: property_value_encoded(&property, new_value)?,
        });

        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    fn graph_is_empty(&self, graph_id: &GraphId) -> Result<bool, RoCrateError> {
        Ok(self.current_triples(graph_id)?.is_empty())
    }

    fn require_rocrate_initialized(&self, graph_id: &GraphId) -> Result<(), RoCrateError> {
        let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
        let dataset = EncodedTerm::from_named_node(&vocab::schema_dataset());
        let current = self.subject_triples(graph_id, ROOT_ID)?;
        if current
            .iter()
            .any(|(predicate, object)| predicate == &rdf_type && object == &dataset)
