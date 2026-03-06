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
