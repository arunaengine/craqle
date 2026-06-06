use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

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
const METADATA_ID: &str = "ro-crate-metadata.json";
type TripleKey = (EncodedTerm, EncodedTerm, EncodedTerm);

fn root_id(graph_id: &GraphId) -> &str {
    graph_id.as_str()
}

fn root_term(graph_id: &GraphId) -> EncodedTerm {
    EncodedTerm::from_named_node(&graph_id.0)
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
        let total_started = Instant::now();
        let result = (|| {
            let root_id = root_id(&graph_id);
            let is_empty = crate::trace_latency_step(
                "craqle.rocrate.create_crate",
                "graph_is_empty",
                &graph_id,
                || self.graph_is_empty(&graph_id),
            )?;
            if is_empty {
                let started = Instant::now();
                let license_value = encoded_license_value(license)?;
                crate::record_latency_step(
                    "craqle.rocrate.create_crate",
                    "encode_license",
                    &graph_id,
                    started,
                    true,
                );

                let started = Instant::now();
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
                        encoded_identifier(root_id),
                    ),
                    insert_change(
                        &graph_id,
                        root_id,
                        &vocab::rdf_type(),
                        EncodedTerm::from_named_node(&vocab::schema_dataset()),
                    ),
                    insert_change(
                        &graph_id,
                        root_id,
                        &vocab::schema_name(),
                        encoded_literal(name),
                    ),
                    insert_change(
                        &graph_id,
                        root_id,
                        &vocab::schema_description(),
                        encoded_literal(description),
                    ),
                    insert_change(
                        &graph_id,
                        root_id,
                        &vocab::schema_date_published(),
                        encoded_literal(date_published),
                    ),
                    insert_change(&graph_id, root_id, &vocab::schema_license(), license_value),
                ];
                crate::record_latency_step(
                    "craqle.rocrate.create_crate",
                    "build_base_changes",
                    &graph_id,
                    started,
                    true,
                );
                tracing::debug!(
                    event = "craqle.rocrate.create_crate.changes",
                    operation = "craqle.rocrate.create_crate",
                    step = "build_base_changes",
                    graph = %graph_id.as_str(),
                    change_count = changes.len() as u64,
                );
                return Ok(crate::trace_latency_step(
                    "craqle.rocrate.create_crate",
                    "local_apply_changes",
                    &graph_id,
                    || self.engine.local_apply_changes(&graph_id, changes),
                )?);
            }

            let started = Instant::now();
            let license = license_from_str(license)?;
            crate::record_latency_step(
                "craqle.rocrate.create_crate",
                "parse_license",
                &graph_id,
                started,
                true,
            );

            let started = Instant::now();
            let rocrate = RoCrate {
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
            };
            crate::record_latency_step(
                "craqle.rocrate.create_crate",
                "build_replacement_rocrate",
                &graph_id,
                started,
                true,
            );

            crate::trace_latency_step(
                "craqle.rocrate.create_crate",
                "replace_graph_with_rocrate",
                &graph_id,
                || self.replace_graph_with_rocrate(&graph_id, rocrate),
            )
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        let batch_ops = result
            .as_ref()
            .map(|batch| batch.ops.len() as u64)
            .unwrap_or(0);
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.rocrate.create_crate",
            graph = %graph_id.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            batch_ops = batch_ops,
        );
        result
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
            root_id(graph_id),
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
        self.append_new_data_entities_under(graph_id, root_id(graph_id), entities)
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
        let current = self.subject_triples(graph_id, root_id(graph_id))?;
        if current
            .iter()
            .any(|(predicate, object)| predicate == &rdf_type && object == &dataset)
        {
            Ok(())
        } else {
            Err(RoCrateError::InvalidGraph(format!(
                "graph `{}` is not initialized as an RO-Crate",
                graph_id.as_str()
            )))
        }
    }

    fn upsert_data_entity_incremental(
        &self,
        graph_id: &GraphId,
        parent_id: &str,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        if self.subject_triples(graph_id, parent_id)?.is_empty() {
            return Err(RoCrateError::EntityNotFound(parent_id.to_string()));
        }
        let mut changes = self.replace_subject_changes(
            graph_id,
            entity_id,
            entity_subject_triples(entity_id, entity_type, name, &additional_triples)?,
        )?;
        if !self.has_part_link(graph_id, parent_id, entity_id)? {
            changes.push(insert_change(
                graph_id,
                parent_id,
                &vocab::schema_has_part(),
                encoded_identifier(entity_id),
            ));
        }
        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    fn replace_subject_changes(
        &self,
        graph_id: &GraphId,
        subject_id: &str,
        desired_triples: Vec<(EncodedTerm, EncodedTerm)>,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        let subject = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        let current = self.subject_triples(graph_id, subject_id)?;
        let desired: BTreeSet<(EncodedTerm, EncodedTerm)> = desired_triples.into_iter().collect();
        let current: BTreeSet<(EncodedTerm, EncodedTerm)> = current.into_iter().collect();

        let mut changes = Vec::new();
        for (predicate, object) in current.difference(&desired) {
            changes.push(MaterializedQuadChange::Delete {
                graph: graph_id.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            });
        }
        for (predicate, object) in desired.difference(&current) {
            changes.push(MaterializedQuadChange::Insert {
                graph: graph_id.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            });
        }
        Ok(changes)
    }

    fn subject_triples(
        &self,
        graph_id: &GraphId,
        subject_id: &str,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
        let orphaned = self.orphaned_entities(graph_id)?;
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let subject_term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        if orphaned.contains(&subject_term) {
            return Ok(Vec::new());
        }
        let Some(subject_tid) = store.lookup_term(&subject_term)? else {
            return Ok(Vec::new());
        };
        Ok(store
            .triples_for_subject(graph_tid, subject_tid)?
            .into_iter()
            .filter(|(_, object)| !orphaned.contains(object))
            .collect())
    }

    fn subject_triples_excluding_predicate(
        &self,
        graph_id: &GraphId,
        subject_id: &str,
        excluded_predicate: &NamedNode,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
        let orphaned = self.orphaned_entities(graph_id)?;
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let subject_term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        if orphaned.contains(&subject_term) {
            return Ok(Vec::new());
        }
        let Some(subject_tid) = store.lookup_term(&subject_term)? else {
            return Ok(Vec::new());
        };
        let excluded_term = EncodedTerm::from_named_node(excluded_predicate);
        let Some(excluded_tid) = store.lookup_term(&excluded_term)? else {
            return Ok(store
                .triples_for_subject(graph_tid, subject_tid)?
                .into_iter()
                .filter(|(_, object)| !orphaned.contains(object))
                .collect());
        };
        Ok(store
            .triples_for_subject_excluding_predicate(graph_tid, subject_tid, excluded_tid)?
            .into_iter()
            .filter(|(_, object)| !orphaned.contains(object))
            .collect())
    }

    fn has_part_link(
        &self,
        graph_id: &GraphId,
        parent_id: &str,
        child_id: &str,
    ) -> Result<bool, RoCrateError> {
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let child = encoded_identifier(child_id);
        Ok(self
            .subject_triples(graph_id, parent_id)?
            .into_iter()
            .any(|(predicate, object)| predicate == has_part && object == child))
    }

    fn build_partial_export_view(
        &self,
        graph_id: &GraphId,
        page_entities: &[EncodedTerm],
    ) -> Result<RoCrate, RoCrateError> {
        let metadata =
            export_metadata_descriptor(graph_id, self.subject_triples(graph_id, METADATA_ID)?)?;
        let root = export_root_entity(
            graph_id,
            self.subject_triples_excluding_predicate(
                graph_id,
                root_id(graph_id),
                &vocab::schema_has_part(),
            )?,
            page_entities,
        )?;

        let mut graph = vec![
            GraphVector::MetadataDescriptor(metadata),
            GraphVector::RootDataEntity(root),
        ];

        for subject_id in self.collect_partial_view_contextual_entities(graph_id, page_entities)? {
            let triples = self.subject_triples(graph_id, &subject_id)?;
            if triples.is_empty() {
                continue;
            }
            graph.push(export_graph_entity(&subject_id, triples)?);
        }

        for entity in page_entities {
            let Some(subject) = entity.to_named_node() else {
                continue;
            };
            let subject_id = subject.as_str().to_string();
            if subject_id == root_id(graph_id) || subject_id == METADATA_ID {
                continue;
            }
            let triples = self.subject_triples(graph_id, &subject_id)?;
            if triples.is_empty() {
                continue;
            }
            graph.push(export_graph_entity(&subject_id, triples)?);
        }

        Ok(RoCrate {
            context: default_context(),
            graph,
        })
    }

    fn current_rocrate(&self, graph_id: &GraphId) -> Result<RoCrate, RoCrateError> {
        self.build_partial_export_view(graph_id, &self.visible_root_linked_data_entities(graph_id)?)
    }

    fn replace_graph_with_rocrate(
        &self,
        graph_id: &GraphId,
        mut rocrate: RoCrate,
    ) -> Result<Batch, RoCrateError> {
        let changes = self.plan_rocrate_replacement(graph_id, &mut rocrate)?;
        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    fn plan_import_value(
        &self,
        graph_id: &GraphId,
        value: serde_json::Value,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(graph_id, &value)?;
        if !self.engine.store().contains_graph(graph_id)? {
            return Ok(insert_changes(graph_id, target));
        }

        let current = self.current_triples(graph_id)?;
        if current.is_empty() {
            return Ok(insert_changes(graph_id, target));
        }

        diff_triples(graph_id, &current, &target)
    }

    fn graph_is_missing_or_empty(&self, graph_id: &GraphId) -> Result<bool, RoCrateError> {
        Ok(self.engine.store().graph_is_empty(graph_id)?)
    }

    fn import_jsonld_into_empty_graph(
        &self,
        graph_id: GraphId,
        value: serde_json::Value,
    ) -> Result<Batch, RoCrateError> {
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(&graph_id, &value)?;
        validate_complete_import_triples(&graph_id, &target)?;
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, insert_changes(&graph_id, target))?;
        self.engine
            .store()
            .set_graph_diagnostics(&graph_id, &crate::core::GraphDiagnostics::default())?;
        Ok(batch)
    }

    fn replace_jsonld_in_existing_graph(
        &self,
        graph_id: GraphId,
        value: serde_json::Value,
    ) -> Result<Batch, RoCrateError> {
        validate_jsonld_import(&value)?;
        let target = jsonld_triples(&graph_id, &value)?;
        validate_complete_import_triples(&graph_id, &target)?;
        let changes = match self.append_like_replace_changes(&graph_id, &target)? {
            Some(changes) => changes,
            None => diff_triples(&graph_id, &self.current_triples(&graph_id)?, &target)?,
        };
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, changes)?;
        self.engine
            .store()
            .set_graph_diagnostics(&graph_id, &crate::core::GraphDiagnostics::default())?;
        Ok(batch)
    }

    fn import_jsonld_into_empty_graph_trusted(
        &self,
        graph_id: GraphId,
        value: serde_json::Value,
    ) -> Result<Batch, RoCrateError> {
        let target = jsonld_triples(&graph_id, &value)?;
        let batch = self
            .engine
            .local_apply_changes_bulk_unchecked(&graph_id, insert_changes(&graph_id, target))?;
        self.engine
            .store()
            .set_graph_diagnostics(&graph_id, &crate::core::GraphDiagnostics::default())?;
        Ok(batch)
    }

    fn plan_rocrate_replacement(
        &self,
        graph_id: &GraphId,
        rocrate: &mut RoCrate,
    ) -> Result<Vec<MaterializedQuadChange>, RoCrateError> {
        normalize_rocrate(rocrate);
        diff_triples(
            graph_id,
            &self.current_triples(graph_id)?,
            &rocrate_triples(rocrate)?,
        )
    }

    fn current_triples(&self, graph_id: &GraphId) -> Result<BTreeSet<TripleKey>, RoCrateError> {
        let orphaned = self.orphaned_entities(graph_id)?;
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(BTreeSet::new());
        };

        let mut triples = BTreeSet::new();
        let mut term_cache = HashMap::new();
        store.for_each_quad_in_graph::<crate::store::StoreError, _>(graph_tid, |quad| {
            let subject = store.decode_term_cached(&mut term_cache, quad.subject)?;
            let predicate = store.decode_term_cached(&mut term_cache, quad.predicate)?;
            let object = store.decode_term_cached(&mut term_cache, quad.object)?;
            if triple_is_visible(&subject, &object, &orphaned) {
                triples.insert((subject, predicate, object));
            }
            Ok(())
        })?;
        Ok(triples)
    }

    fn append_like_replace_changes(
        &self,
        graph_id: &GraphId,
        target: &BTreeSet<TripleKey>,
    ) -> Result<Option<Vec<MaterializedQuadChange>>, RoCrateError> {
        let orphaned = self.orphaned_entities(graph_id)?;
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Some(insert_changes(graph_id, target.clone())));
        };

        let mut remaining = target.clone();
        let mut term_cache = HashMap::new();
        let append_like =
            match store.for_each_quad_in_graph::<AppendLikeCheckError, _>(graph_tid, |quad| {
                let subject = store.decode_term_cached(&mut term_cache, quad.subject)?;
                let predicate = store.decode_term_cached(&mut term_cache, quad.predicate)?;
                let object = store.decode_term_cached(&mut term_cache, quad.object)?;
                if !triple_is_visible(&subject, &object, &orphaned) {
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
        graph_id: &GraphId,
        subject_id: &str,
    ) -> Result<bool, RoCrateError> {
        let subject = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        if self.orphaned_entities(graph_id)?.contains(&subject) {
            return Ok(false);
        }
        Ok(self.engine.store().contains_subject(graph_id, &subject)?)
    }

    fn orphaned_entities(&self, graph_id: &GraphId) -> Result<HashSet<EncodedTerm>, RoCrateError> {
        Ok(self
            .engine
            .store()
            .graph_diagnostics(graph_id)?
            .orphaned_entities
            .into_iter()
            .map(|entity_id| EncodedTerm::from_named_node(&NamedNode::new_unchecked(&entity_id)))
            .collect())
    }

    fn visible_root_linked_data_entities(
        &self,
        graph_id: &GraphId,
    ) -> Result<Vec<EncodedTerm>, RoCrateError> {
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let mut entities: Vec<EncodedTerm> = self
            .subject_triples(graph_id, root_id(graph_id))?
            .into_iter()
            .filter(|(predicate, _)| predicate == &has_part)
            .map(|(_, object)| object)
            .collect();
        entities.sort();
        entities.dedup();
        Ok(entities)
    }

    fn root_linked_data_entity_page(
        &self,
        graph_id: &GraphId,
        offset: usize,
        limit: usize,
    ) -> Result<(usize, Vec<EncodedTerm>), RoCrateError> {
        if self.orphaned_entities(graph_id)?.is_empty() {
            let root = root_term(graph_id);
            let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
            return Ok(self
                .engine
                .store()
                .objects_for_subject_predicate_page(graph_id, &root, &has_part, offset, limit)?);
        }

        let visible = self.visible_root_linked_data_entities(graph_id)?;
        let total = visible.len();
        let page = visible.into_iter().skip(offset).take(limit).collect();
        Ok((total, page))
    }

    fn root_linked_data_entity_page_after(
        &self,
        graph_id: &GraphId,
        after_entity_id: Option<&str>,
        limit: usize,
    ) -> Result<(usize, Vec<EncodedTerm>), RoCrateError> {
        let after = after_entity_id.map(normalize_entity_id);
        if self.orphaned_entities(graph_id)?.is_empty() {
            let root = root_term(graph_id);
            let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
            let after = after
                .as_ref()
                .map(|id| EncodedTerm::from_named_node(&NamedNode::new_unchecked(id.as_str())));
            let total = self
                .engine
                .store()
                .count_objects_for_subject_predicate(graph_id, &root, &has_part)?;
            let page = self
                .engine
                .store()
                .objects_for_subject_predicate_page_after(
                    graph_id,
                    &root,
                    &has_part,
                    after.as_ref(),
                    limit,
                )?;
            return Ok((total, page));
        }

        let visible = self.visible_root_linked_data_entities(graph_id)?;
        let total = visible.len();
        let mut page: Vec<EncodedTerm> = visible
            .into_iter()
            .skip_while(|entity| {
                after.as_deref().is_some_and(|after_id| {
                    encoded_named_node_value(entity).as_deref() != Some(after_id)
                })
            })
            .collect();
        if after.is_some() && !page.is_empty() {
            page.remove(0);
        }
        page.truncate(limit);
        Ok((total, page))
    }

    fn collect_partial_view_contextual_entities(
        &self,
        graph_id: &GraphId,
        page_entities: &[EncodedTerm],
    ) -> Result<Vec<String>, RoCrateError> {
        let page_subjects: HashSet<String> = page_entities
            .iter()
            .filter_map(encoded_named_node_value)
            .collect();
        let mut queue = VecDeque::from([METADATA_ID.to_string(), root_id(graph_id).to_string()]);
        queue.extend(page_subjects.iter().cloned());

        let mut expanded = HashSet::new();
        let mut contextuals = BTreeSet::new();

        while let Some(subject_id) = queue.pop_front() {
            if !expanded.insert(subject_id.clone()) {
                continue;
            }

            let references = if subject_id == root_id(graph_id) {
                self.subject_triples_excluding_predicate(
                    graph_id,
                    root_id(graph_id),
                    &vocab::schema_has_part(),
                )?
            } else {
                self.subject_triples(graph_id, &subject_id)?
            };

            for (_, object) in references {
                let Some(candidate_id) = encoded_named_node_value(&object) else {
                    continue;
                };
                if candidate_id == root_id(graph_id)
                    || candidate_id == METADATA_ID
                    || page_subjects.contains(&candidate_id)
                {
                    continue;
                }

                let triples = self.subject_triples(graph_id, &candidate_id)?;
                if triples.is_empty() || !triples_describe_contextual_entity(&triples) {
                    continue;
                }

                if contextuals.insert(candidate_id.clone()) {
                    queue.push_back(candidate_id);
                }
            }
        }

        Ok(contextuals.into_iter().collect())
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
        subject: EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id)),
        predicate: EncodedTerm::from_named_node(predicate),
        object,
    }
}

fn triple_is_visible(
    subject: &EncodedTerm,
    object: &EncodedTerm,
    orphaned: &HashSet<EncodedTerm>,
) -> bool {
    !orphaned.contains(subject) && !orphaned.contains(object)
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

fn validate_complete_import_triples(
    graph_id: &GraphId,
    triples: &BTreeSet<TripleKey>,
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
    let root_license = EncodedTerm::from_named_node(&vocab::schema_license());

    let mut subjects = BTreeSet::new();
    let mut typed_subjects = HashSet::new();
    let mut adjacency: HashMap<EncodedTerm, Vec<EncodedTerm>> = HashMap::new();
    let mut data_entities = BTreeSet::new();
    let mut has_root_dataset = false;
    let mut has_metadata_type = false;
    let mut root_name_count = 0usize;
    let mut root_description_count = 0usize;
    let mut root_date_published_count = 0usize;
    let mut root_license_count = 0usize;

    for (subject, predicate, object) in triples {
        subjects.insert(subject.clone());

        if subject == &root && predicate == &root_name {
            root_name_count += 1;
        }
        if subject == &root && predicate == &root_description {
            root_description_count += 1;
        }
        if subject == &root && predicate == &root_date_published {
            root_date_published_count += 1;
        }
        if subject == &root && predicate == &root_license {
            root_license_count += 1;
        }

        if predicate == &rdf_type {
            typed_subjects.insert(subject.clone());
            if subject == &root && object == &dataset {
                has_root_dataset = true;
            }
            if subject == &metadata && object == &creative_work {
                has_metadata_type = true;
            }
            if subject != &root && (object == &dataset || object == &media_object) {
                data_entities.insert(subject.clone());
            }
        }

        if predicate == &has_part {
            adjacency
                .entry(subject.clone())
                .or_default()
                .push(object.clone());
            if subject != &root {
                data_entities.insert(subject.clone());
            }
            if object != &root {
                data_entities.insert(object.clone());
            }
        }
    }

    let mut violations = Vec::new();
    let metadata_about_root = triples.contains(&(metadata.clone(), about.clone(), root.clone()));

    if !has_root_dataset {
        violations.push(crate::core::CrateViolation::MissingRootDataEntity);
    }
    if !(has_metadata_type && metadata_about_root) {
        violations.push(crate::core::CrateViolation::MissingMetadataDescriptor);
    }
    if root_name_count < 1 {
        violations.push(crate::core::CrateViolation::MissingRequiredProperty {
            entity: root_id(graph_id).to_string(),
            property: "schema:name".to_string(),
        });
    }
    if root_description_count < 1 {
        violations.push(crate::core::CrateViolation::MissingRequiredProperty {
            entity: root_id(graph_id).to_string(),
            property: "schema:description".to_string(),
        });
    }
    if root_date_published_count < 1 {
        violations.push(crate::core::CrateViolation::MissingRequiredProperty {
            entity: root_id(graph_id).to_string(),
            property: "schema:datePublished".to_string(),
        });
    }
    if root_license_count < 1 {
        violations.push(crate::core::CrateViolation::MissingRequiredProperty {
            entity: root_id(graph_id).to_string(),
            property: "schema:license".to_string(),
        });
    }
    if root_date_published_count != 1 {
        violations.push(
            crate::core::CrateViolation::InvalidDatePublishedCardinality {
                count: root_date_published_count,
            },
        );
    }

    if let Some(subject) = subjects
        .iter()
        .find(|subject| !typed_subjects.contains(*subject))
    {
        violations.push(crate::core::CrateViolation::EntityMissingType {
            entity_id: subject.0.clone(),
        });
    }

    let mut reachable = HashSet::new();
    let mut queue = VecDeque::from([root.clone()]);
    reachable.insert(root.clone());
    while let Some(current) = queue.pop_front() {
        if let Some(children) = adjacency.get(&current) {
            for child in children {
                if reachable.insert(child.clone()) {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    if let Some(orphan) = data_entities
        .into_iter()
        .find(|entity| !reachable.contains(entity))
    {
        violations.push(crate::core::CrateViolation::OrphanedDataEntity {
            entity_id: orphan.0,
        });
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

    let graph = object
        .get("@graph")
        .or_else(|| object.get("graph"))
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

        for (property, property_value) in entity {
            if matches!(
                property.as_str(),
                "@id" | "id" | "@type" | "type" | "@context"
            ) {
                continue;
            }
            validate_property_value(property, property_value)?;
        }
    }

    Ok(())
}

fn validate_property_value(property: &str, value: &serde_json::Value) -> Result<(), RoCrateError> {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => Ok(()),
        serde_json::Value::Array(values) => {
            for entry in values {
                validate_property_value(property, entry)?;
            }
            Ok(())
        }
        serde_json::Value::Object(object) if is_reference_object(object) => Ok(()),
        serde_json::Value::Object(object) if is_value_object(object) => {
            validate_value_object(property, object)
        }
        serde_json::Value::Object(_) => Err(RoCrateError::UnsupportedJsonLd(format!(
            "property `{property}` contains an inline nested object; nested entities must be top-level `@graph` entries referenced by `@id`"
        ))),
    }
}

fn is_reference_object(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    let has_identifier = object.contains_key("@id") || object.contains_key("id");
    has_identifier
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "@id" | "id" | "@type" | "type"))
}

fn is_value_object(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    let has_value = object.contains_key("@value") || object.contains_key("value");
    has_value
        && object.keys().all(|key| {
            matches!(
                key.as_str(),
                "@value" | "value" | "@type" | "type" | "@language" | "language"
            )
        })
}

fn validate_value_object(
    property: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RoCrateError> {
    let value = object
        .get("@value")
        .or_else(|| object.get("value"))
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(format!(
                "property `{property}` value object is missing `@value`"
            ))
        })?;

    if matches!(
        value,
        serde_json::Value::Array(_) | serde_json::Value::Object(_)
    ) {
        return Err(RoCrateError::UnsupportedJsonLd(format!(
            "property `{property}` value object must contain a scalar `@value`"
        )));
    }

    let language = object.get("@language").or_else(|| object.get("language"));
    let datatype = object.get("@type").or_else(|| object.get("type"));

    if language.is_some() && datatype.is_some() {
        return Err(RoCrateError::UnsupportedJsonLd(format!(
            "property `{property}` value object must not combine `@language` and `@type`"
        )));
    }

    if let Some(language) = language {
        if !matches!(value, serde_json::Value::String(_)) {
            return Err(RoCrateError::UnsupportedJsonLd(format!(
                "property `{property}` language-tagged values must use string `@value`"
            )));
        }
        if !language.is_string() {
            return Err(RoCrateError::UnsupportedJsonLd(format!(
                "property `{property}` language tag must be a string"
            )));
        }
    }

    if let Some(datatype) = datatype {
        let Some(datatype) = datatype.as_str() else {
            return Err(RoCrateError::UnsupportedJsonLd(format!(
                "property `{property}` datatype must be a string"
            )));
        };
        let _ = if datatype.starts_with("http://") || datatype.starts_with("https://") {
            NamedNode::new_unchecked(datatype)
        } else {
            expand_known_compact_iri(datatype)?
        };
    }

    Ok(())
}

fn jsonld_triples(
    graph_id: &GraphId,
    value: &serde_json::Value,
) -> Result<BTreeSet<TripleKey>, RoCrateError> {
    let object = value.as_object().ok_or_else(|| {
        RoCrateError::UnsupportedJsonLd("top-level JSON-LD document must be an object".to_string())
    })?;
    let graph = object
        .get("@graph")
        .or_else(|| object.get("graph"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(
                "RO-Crate import requires a top-level `@graph` array".to_string(),
            )
        })?;

    let import_root = document_root_id(graph);
    let mut triples = BTreeSet::new();
    for (index, entry) in graph.iter().enumerate() {
        let entity = entry.as_object().ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(format!("@graph entry {index} must be an object"))
        })?;
        let subject_id = entity_identifier(graph_id, import_root.as_deref(), entity, index)?;
        let subject = EncodedTerm::from_named_node(&NamedNode::new_unchecked(&subject_id));

        if let Some(type_value) = entity.get("@type").or_else(|| entity.get("type")) {
            let predicate = EncodedTerm::from_named_node(&vocab::rdf_type());
            for object in
                property_value_terms(graph_id, import_root.as_deref(), "type", type_value)?
            {
                triples.insert((subject.clone(), predicate.clone(), object));
            }
        }

        for (property, property_value) in entity {
            if matches!(
                property.as_str(),
                "@context" | "@graph" | "graph" | "@id" | "id" | "@type" | "type"
            ) {
                continue;
            }
            let normalized_property = normalize_property(property);
            let predicate =
                EncodedTerm::from_named_node(&property_named_node(&normalized_property)?);
            for object in property_value_terms(
                graph_id,
                import_root.as_deref(),
                &normalized_property,
                property_value,
            )? {
                triples.insert((subject.clone(), predicate.clone(), object));
            }
        }
    }

    Ok(triples)
}

fn entity_identifier(
    graph_id: &GraphId,
    import_root: Option<&str>,
    entity: &serde_json::Map<String, serde_json::Value>,
    index: usize,
) -> Result<String, RoCrateError> {
    let raw = entity
        .get("@id")
        .or_else(|| entity.get("id"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd(format!(
                "@graph entry {index} must define string `@id`"
            ))
        })?;
    Ok(remap_import_identifier(
        graph_id,
        import_root,
        &normalize_entity_id(raw),
    ))
}

fn document_root_id(graph: &[serde_json::Value]) -> Option<String> {
    graph.iter().find_map(|entry| {
        let entity = entry.as_object()?;
        let id = entity
            .get("@id")
            .or_else(|| entity.get("id"))
            .and_then(serde_json::Value::as_str)?;
        if id != METADATA_ID {
            return None;
        }

        let about = entity.get("about")?;
        match about {
            serde_json::Value::String(value) => Some(normalize_entity_id(value)),
            serde_json::Value::Object(object) => object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(normalize_entity_id),
            _ => None,
        }
    })
}

fn remap_import_identifier(graph_id: &GraphId, import_root: Option<&str>, id: &str) -> String {
    if import_root == Some(id) {
        root_id(graph_id).to_string()
    } else {
        id.to_string()
    }
}

fn property_expects_identifier(property: &str) -> bool {
    matches!(property, "license" | "about" | "conformsTo")
}

fn property_value_terms(
    graph_id: &GraphId,
    import_root: Option<&str>,
    property: &str,
    value: &serde_json::Value,
) -> Result<Vec<EncodedTerm>, RoCrateError> {
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Bool(boolean) => Ok(vec![encoded_typed_literal(
            boolean.to_string(),
            XSD_BOOLEAN_IRI,
        )]),
        serde_json::Value::Number(number) => Ok(vec![encoded_number_literal(number)]),
        serde_json::Value::String(text) => {
            let mapped = remap_import_identifier(graph_id, import_root, &normalize_entity_id(text));
            let value = if property_expects_identifier(property) {
                mapped.as_str()
            } else {
                text
            };
            Ok(vec![property_value_encoded(property, value)?])
        }
        serde_json::Value::Array(values) => {
            let mut objects = Vec::new();
            for entry in values {
                objects.extend(property_value_terms(
                    graph_id,
                    import_root,
                    property,
                    entry,
                )?);
            }
            Ok(objects)
        }
        serde_json::Value::Object(object) if is_reference_object(object) => {
            let id = object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    RoCrateError::UnsupportedJsonLd(format!(
                        "property `{property}` reference object is missing string `@id`"
                    ))
                })?;
            let mapped = remap_import_identifier(graph_id, import_root, &normalize_entity_id(id));
            Ok(vec![encoded_reference_term(&mapped)?])
        }
        serde_json::Value::Object(object) if is_value_object(object) => {
            Ok(vec![encoded_value_object(object)?])
        }
        serde_json::Value::Object(_) => Err(RoCrateError::UnsupportedJsonLd(format!(
            "property `{property}` contains an inline nested object; nested entities must be top-level `@graph` entries referenced by `@id`"
        ))),
    }
}

fn encoded_number_literal(number: &serde_json::Number) -> EncodedTerm {
    if number.as_i64().is_some() || number.as_u64().is_some() {
        encoded_typed_literal(number.to_string(), XSD_INTEGER_IRI)
    } else {
        encoded_typed_literal(number.to_string(), XSD_DOUBLE_IRI)
    }
}

fn encoded_value_object(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<EncodedTerm, RoCrateError> {
    let value = object
        .get("@value")
        .or_else(|| object.get("value"))
        .ok_or_else(|| {
            RoCrateError::UnsupportedJsonLd("value object missing `@value`".to_string())
        })?;
    let language = object
        .get("@language")
        .or_else(|| object.get("language"))
        .and_then(serde_json::Value::as_str);
    let datatype = object
        .get("@type")
        .or_else(|| object.get("type"))
        .and_then(serde_json::Value::as_str);

    match value {
        serde_json::Value::String(text) => {
            if let Some(language) = language {
                Ok(encoded_language_literal(text, language))
            } else if let Some(datatype) = datatype {
                Ok(encoded_typed_literal(text.clone(), datatype_iri(datatype)?))
            } else {
                Ok(encoded_literal(text))
            }
        }
        serde_json::Value::Bool(boolean) => Ok(encoded_typed_literal(
            boolean.to_string(),
            datatype
                .map(datatype_iri)
                .transpose()?
                .unwrap_or_else(|| XSD_BOOLEAN_IRI.to_string()),
        )),
        serde_json::Value::Number(number) => Ok(encoded_typed_literal(
            number.to_string(),
            datatype.map(datatype_iri).transpose()?.unwrap_or_else(|| {
                if number.as_i64().is_some() || number.as_u64().is_some() {
                    XSD_INTEGER_IRI.to_string()
                } else {
                    XSD_DOUBLE_IRI.to_string()
                }
            }),
        )),
        serde_json::Value::Null => Ok(encoded_literal("")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            RoCrateError::UnsupportedJsonLd("value object `@value` must be scalar".to_string()),
        ),
    }
}

fn datatype_iri(datatype: &str) -> Result<String, RoCrateError> {
    if datatype.starts_with("http://") || datatype.starts_with("https://") {
        Ok(datatype.to_string())
    } else {
        Ok(expand_known_compact_iri(datatype)?.as_str().to_string())
    }
}

fn entity_subject_triples(
    _entity_id: &str,
    entity_type: &str,
    name: &str,
    additional_triples: &[(NamedNode, oxrdf::Term)],
) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
    let mut triples = vec![
        (
            EncodedTerm::from_named_node(&vocab::rdf_type()),
            encoded_class_term(entity_type)?,
        ),
        (
            EncodedTerm::from_named_node(&vocab::schema_name()),
            encoded_literal(name),
        ),
    ];

    for (predicate, object) in additional_triples {
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
        "conformsTo" => Ok(vocab::schema_conforms_to()),
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

fn encoded_identifier(value: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(value))
}

fn encoded_literal(value: &str) -> EncodedTerm {
    EncodedTerm::from_term(&Term::Literal(oxrdf::Literal::new_simple_literal(value)))
}

fn encoded_typed_literal(value: impl Into<String>, datatype: impl AsRef<str>) -> EncodedTerm {
    EncodedTerm::from_term(&Term::Literal(oxrdf::Literal::new_typed_literal(
        value.into(),
        NamedNode::new_unchecked(datatype.as_ref()),
    )))
}

fn encoded_language_literal(value: &str, language: &str) -> EncodedTerm {
    EncodedTerm::from_term(&Term::Literal(
        oxrdf::Literal::new_language_tagged_literal_unchecked(value, language),
    ))
}

fn encoded_reference_term(value: &str) -> Result<EncodedTerm, RoCrateError> {
    let is_identifier = value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || NamedNode::new(value).is_ok();

    if is_identifier {
        Ok(encoded_identifier(value))
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
    graph_id: &GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm)>,
) -> Result<MetadataDescriptor, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut conforms_to = None;
    let mut about = None;
    let mut dynamic = HashMap::new();

    for (predicate, object) in triples {
        let key = predicate_key(&predicate);
        match key.as_str() {
            "type" | "@type" => {
                if let Some(value) = object_named_node_value(&object) {
                    type_terms.push(value);
                }
            }
            "conformsTo" => conforms_to = Some(id_from_encoded_term(&object)),
            "about" => about = Some(id_from_encoded_term(&object)),
            _ => insert_entity_value(&mut dynamic, key, entity_value_from_encoded_term(&object)),
        }
    }

    Ok(MetadataDescriptor {
        id: METADATA_ID.to_string(),
        type_: data_type_from_terms(type_terms, "CreativeWork"),
        conforms_to: conforms_to.unwrap_or_else(|| Id::Id(ROCRATE_SPEC_URL.to_string())),
        about: about.unwrap_or_else(|| Id::Id(root_id(graph_id).to_string())),
        dynamic_entity: (!dynamic.is_empty()).then_some(dynamic),
    })
}

fn export_root_entity(
    graph_id: &GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm)>,
    page_entities: &[EncodedTerm],
) -> Result<RootDataEntity, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut name = None;
    let mut description = None;
    let mut date_published = None;
    let mut license = None;
    let mut dynamic = HashMap::new();

    for (predicate, object) in triples {
        let key = predicate_key(&predicate);
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
            _ => insert_entity_value(&mut dynamic, key, entity_value_from_encoded_term(&object)),
        }
    }

    if !page_entities.is_empty() {
        let ids = page_entities
            .iter()
            .filter_map(|term| term.to_named_node().map(|node| node.as_str().to_string()))
            .collect::<Vec<_>>();
        if !ids.is_empty() {
            dynamic.insert(
                "hasPart".to_string(),
                if ids.len() == 1 {
                    EntityValue::EntityId(Id::Id(ids[0].clone()))
                } else {
                    EntityValue::EntityId(Id::IdArray(ids))
                },
            );
        }
    }

    Ok(RootDataEntity {
        id: root_id(graph_id).to_string(),
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
) -> Result<GraphVector, RoCrateError> {
    let mut type_terms = Vec::new();
    let mut dynamic = HashMap::new();

    for (predicate, object) in triples {
        let key = predicate_key(&predicate);
        match key.as_str() {
            "type" | "@type" => {
                if let Some(value) = object_named_node_value(&object) {
                    type_terms.push(value);
                }
            }
            _ => insert_entity_value(&mut dynamic, key, entity_value_from_encoded_term(&object)),
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

fn predicate_key(predicate: &EncodedTerm) -> String {
    predicate
        .to_named_node()
        .map(|node| normalize_compact_term(node.as_str()))
        .unwrap_or_else(|| predicate.0.clone())
}

fn object_named_node_value(object: &EncodedTerm) -> Option<String> {
    object
        .to_named_node()
        .map(|node| normalize_compact_term(node.as_str()))
}

fn encoded_named_node_value(object: &EncodedTerm) -> Option<String> {
    object.to_named_node().map(|node| node.as_str().to_string())
}

fn normalize_compact_term(value: &str) -> String {
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
