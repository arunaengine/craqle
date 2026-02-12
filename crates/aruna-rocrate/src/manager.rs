use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use aruna_core::{Batch, EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use aruna_repl::ReplicationEngine;
use oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use rocraters::ro_crate::constraints::{DataType, EntityValue, Id, License};
use rocraters::ro_crate::context::RoCrateContext;
use rocraters::ro_crate::contextual_entity::ContextualEntity;
use rocraters::ro_crate::data_entity::DataEntity;
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use rocraters::ro_crate::rdf::{
    ContextResolverBuilder, ConversionOptions, RdfError, RdfGraph, rdf_graph_to_rocrate,
    rocrate_to_rdf_with_options,
};
use rocraters::ro_crate::rocrate::RoCrate;
use rocraters::ro_crate::root::RootDataEntity;

const ROCRATE_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.2/context";
const ROCRATE_SPEC_URL: &str = "https://w3id.org/ro/crate/1.2";
const ROOT_ID: &str = "./";
const METADATA_ID: &str = "ro-crate-metadata.json";
type TripleKey = (EncodedTerm, EncodedTerm, EncodedTerm);

#[derive(Debug, thiserror::Error)]
pub enum RoCrateError {
    #[error("update: {0}")]
    Update(#[from] aruna_repl::UpdateError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("store: {0}")]
    Store(#[from] aruna_rdf_store::StoreError),
    #[error("rdf: {0}")]
    Rdf(#[from] RdfError),
    #[error("invalid graph: {0}")]
    InvalidGraph(String),
    #[error("entity not found: {0}")]
    EntityNotFound(String),
}

#[derive(Debug, Clone)]
pub struct BenchmarkExportPage {
    pub jsonld: String,
    pub total_data_entities: usize,
    pub returned_data_entities: usize,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
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
                    encoded_license_value(license),
                ),
            ];
            return Ok(self
                .engine
                .local_apply_changes_unchecked(&graph_id, changes)?);
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
                    license: license_from_str(license),
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

    /// Add a data entity linked from an arbitrary parent dataset/entity.
    pub fn add_data_entity_under(
        &self,
        graph_id: &GraphId,
        parent_id: &str,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, oxrdf::Term)>,
    ) -> Result<Batch, RoCrateError> {
        self.require_rocrate_initialized(graph_id)?;
        let parent_id = normalize_entity_id(parent_id);
        let entity_id = normalize_entity_id(entity_id);
        self.upsert_data_entity_incremental(
            graph_id,
            &parent_id,
            &entity_id,
            entity_type,
            name,
            additional_triples,
        )
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
            entity_subject_triples(&entity_id, entity_type, name, &additional_triples),
        )?;
        Ok(self
            .engine
            .local_apply_changes_unchecked(graph_id, std::mem::take(&mut changes))?)
    }

    /// Export a graph to RO-Crate JSON-LD using `ro-crate-rs`.
    pub fn export_jsonld(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        Ok(serde_json::to_string_pretty(
            &self.current_rocrate(graph_id)?,
        )?)
    }

    /// Export a lightweight partial RO-Crate view without data entities.
    pub fn export_jsonld_summary(&self, graph_id: &GraphId) -> Result<String, RoCrateError> {
        Ok(serde_json::to_string(
            &self.build_benchmark_view(graph_id, &[])?,
        )?)
    }

    /// Export an offset-based partial RO-Crate page of root-linked data entities.
    pub fn export_jsonld_page(
        &self,
        graph_id: &GraphId,
        offset: usize,
        limit: usize,
    ) -> Result<BenchmarkExportPage, RoCrateError> {
        let root = EncodedTerm::from_named_node(&vocab::root_entity());
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let (total, page) = self
            .engine
            .store()
            .objects_for_subject_predicate_page(graph_id, &root, &has_part, offset, limit)?;
        let jsonld = serde_json::to_string(&self.build_benchmark_view(graph_id, &page)?)?;
        let returned = page.len();
        let has_more = offset + returned < total;
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_named_node_value))
            .flatten();

        Ok(BenchmarkExportPage {
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
    ) -> Result<BenchmarkExportPage, RoCrateError> {
        let root = EncodedTerm::from_named_node(&vocab::root_entity());
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let total = self
            .engine
            .store()
            .count_objects_for_subject_predicate(graph_id, &root, &has_part)?;
        let after = after_entity_id.map(normalize_entity_id);
        let after_term = after.as_deref().map(encoded_identifier);
        let mut page = self
            .engine
            .store()
            .objects_for_subject_predicate_page_after(
                graph_id,
                &root,
                &has_part,
                after_term.as_ref(),
                limit.saturating_add(1),
            )?;
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        let returned = page.len();
        let next_cursor = has_more
            .then(|| page.last().and_then(encoded_named_node_value))
            .flatten();
        let jsonld = serde_json::to_string(&self.build_benchmark_view(graph_id, &page)?)?;

        Ok(BenchmarkExportPage {
            jsonld,
            total_data_entities: total,
            returned_data_entities: returned,
            next_offset: None,
            next_cursor,
        })
    }

    /// Import a JSON-LD RO-Crate metadata file into a named graph.
    pub fn import_jsonld(&self, graph_id: GraphId, jsonld: &str) -> Result<Batch, RoCrateError> {
        let rocrate: RoCrate = serde_json::from_str(jsonld)?;
        self.replace_graph_with_rocrate(&graph_id, rocrate)
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
                let old_object = property_value_encoded(&property, old_value);
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
            object: property_value_encoded(&property, new_value),
        });

        Ok(self
            .engine
            .local_apply_changes_unchecked(graph_id, changes)?)
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
            entity_subject_triples(entity_id, entity_type, name, &additional_triples),
        )?;
        if !self.has_part_link(graph_id, parent_id, entity_id)? {
            changes.push(insert_change(
                graph_id,
                parent_id,
                &vocab::schema_has_part(),
                encoded_identifier(entity_id),
            ));
        }
        Ok(self
            .engine
            .local_apply_changes_unchecked(graph_id, changes)?)
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
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let subject_term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        let Some(subject_tid) = store.lookup_term(&subject_term)? else {
            return Ok(Vec::new());
        };
        Ok(store.triples_for_subject(graph_tid, subject_tid)?)
    }

    fn subject_triples_excluding_predicate(
        &self,
        graph_id: &GraphId,
        subject_id: &str,
        excluded_predicate: &NamedNode,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>, RoCrateError> {
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Vec::new());
        };
        let subject_term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(subject_id));
        let Some(subject_tid) = store.lookup_term(&subject_term)? else {
            return Ok(Vec::new());
        };
        let excluded_term = EncodedTerm::from_named_node(excluded_predicate);
        let Some(excluded_tid) = store.lookup_term(&excluded_term)? else {
            return Ok(store.triples_for_subject(graph_tid, subject_tid)?);
        };
        Ok(store.triples_for_subject_excluding_predicate(graph_tid, subject_tid, excluded_tid)?)
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

    fn build_benchmark_view(
        &self,
        graph_id: &GraphId,
        page_entities: &[EncodedTerm],
    ) -> Result<RoCrate, RoCrateError> {
        let metadata = benchmark_metadata_descriptor(self.subject_triples(graph_id, METADATA_ID)?)?;
        let root = benchmark_root_entity(
            self.subject_triples_excluding_predicate(graph_id, ROOT_ID, &vocab::schema_has_part())?,
            page_entities,
        )?;

        let mut graph = vec![
            GraphVector::MetadataDescriptor(metadata),
            GraphVector::RootDataEntity(root),
        ];

        for entity in page_entities {
            let Some(subject) = entity.to_named_node() else {
                continue;
            };
            let subject_id = subject.as_str().to_string();
            if subject_id == ROOT_ID || subject_id == METADATA_ID {
                continue;
            }
            let triples = self.subject_triples(graph_id, &subject_id)?;
            if triples.is_empty() {
                continue;
            }
            graph.push(benchmark_entity(&subject_id, triples)?);
        }

        Ok(RoCrate {
            context: default_context(),
            graph,
        })
    }

    fn current_rocrate(&self, graph_id: &GraphId) -> Result<RoCrate, RoCrateError> {
        let mut rocrate = rdf_graph_to_rocrate(self.store_rdf_graph(graph_id)?)?;
        normalize_rocrate(&mut rocrate);
        Ok(rocrate)
    }

    fn replace_graph_with_rocrate(
        &self,
        graph_id: &GraphId,
        mut rocrate: RoCrate,
    ) -> Result<Batch, RoCrateError> {
        normalize_rocrate(&mut rocrate);
        let current = self.current_triples(graph_id)?;
        let target = rocrate_triples(&rocrate)?;

        let mut changes = Vec::new();
        for triple in current.difference(&target) {
            changes.push(MaterializedQuadChange::Delete {
                graph: graph_id.clone(),
                subject: triple.0.clone(),
                predicate: triple.1.clone(),
                object: triple.2.clone(),
            });
        }
        for triple in target.difference(&current) {
            changes.push(MaterializedQuadChange::Insert {
                graph: graph_id.clone(),
                subject: triple.0.clone(),
                predicate: triple.1.clone(),
                object: triple.2.clone(),
            });
        }

        Ok(self.engine.local_apply_changes(graph_id, changes)?)
    }

    fn current_triples(&self, graph_id: &GraphId) -> Result<BTreeSet<TripleKey>, RoCrateError> {
        let store = self.engine.store();
        let graph_term = EncodedTerm::from_named_node(&graph_id.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(BTreeSet::new());
        };

        let mut triples = BTreeSet::new();
        for quad in store.quads_for_pattern(Some(graph_tid), None, None, None)? {
            triples.insert((
                store.decode_term(quad.subject)?,
                store.decode_term(quad.predicate)?,
                store.decode_term(quad.object)?,
            ));
        }
        Ok(triples)
    }

    fn store_rdf_graph(&self, graph_id: &GraphId) -> Result<RdfGraph, RoCrateError> {
        let context = ContextResolverBuilder::default()
            .resolve(&default_context())
            .map_err(RdfError::from)?;
        let mut rdf_graph = RdfGraph::new(context);

        for (subject, predicate, object) in self.current_triples(graph_id)? {
            rdf_graph.insert(Triple::new(
                encoded_subject(&subject)?,
                encoded_predicate(&predicate)?,
                encoded_object(&object)?,
            ));
        }

        Ok(rdf_graph)
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

fn entity_subject_triples(
    _entity_id: &str,
    entity_type: &str,
    name: &str,
    additional_triples: &[(NamedNode, oxrdf::Term)],
) -> Vec<(EncodedTerm, EncodedTerm)> {
    let mut triples = vec![
        (
            EncodedTerm::from_named_node(&vocab::rdf_type()),
            encoded_class_term(entity_type),
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

    triples
}

fn property_named_node(property: &str) -> Result<NamedNode, RoCrateError> {
    match property {
        "@type" | "type" => Ok(vocab::rdf_type()),
        "name" => Ok(vocab::schema_name()),
        "description" => Ok(vocab::schema_description()),
        "datePublished" => Ok(vocab::schema_date_published()),
        "license" => Ok(vocab::schema_license()),
        "about" => Ok(vocab::schema_about()),
        "conformsTo" => Ok(vocab::schema_conforms_to()),
        other if other.contains("://") => Ok(NamedNode::new_unchecked(other)),
        other if other.contains(':') => Ok(expand_compact_iri(other)),
        other => Ok(NamedNode::new_unchecked(&format!(
            "http://schema.org/{other}"
        ))),
    }
}

fn property_value_encoded(property: &str, value: &str) -> EncodedTerm {
    match property {
        "@type" | "type" => encoded_class_term(value),
        "license" | "about" | "conformsTo" => {
            if looks_like_identifier(value) {
                encoded_identifier(value)
            } else {
                encoded_literal(value)
            }
        }
        _ => encoded_literal(value),
    }
}

fn encoded_class_term(value: &str) -> EncodedTerm {
    let iri = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else if value.starts_with("schema:")
        || value.starts_with("rdf:")
        || value.starts_with("rdfs:")
    {
        expand_compact_iri(value).as_str().to_string()
    } else {
        format!("http://schema.org/{}", normalize_term(value))
    };
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(&iri))
}

fn encoded_identifier(value: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(value))
}

fn encoded_literal(value: &str) -> EncodedTerm {
    EncodedTerm::from_term(&Term::Literal(oxrdf::Literal::new_simple_literal(value)))
}

fn encoded_license_value(license: &str) -> EncodedTerm {
    if looks_like_identifier(license) {
        encoded_identifier(license)
    } else {
        encoded_literal(license)
    }
}

fn expand_compact_iri(value: &str) -> NamedNode {
    if let Some(local) = value.strip_prefix("schema:") {
        NamedNode::new_unchecked(&format!("http://schema.org/{local}"))
    } else if let Some(local) = value.strip_prefix("rdf:") {
        NamedNode::new_unchecked(&format!(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}"
        ))
    } else if let Some(local) = value.strip_prefix("rdfs:") {
        NamedNode::new_unchecked(&format!("http://www.w3.org/2000/01/rdf-schema#{local}"))
    } else {
        NamedNode::new_unchecked(value)
    }
}

fn benchmark_metadata_descriptor(
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
        about: about.unwrap_or_else(|| Id::Id(ROOT_ID.to_string())),
        dynamic_entity: (!dynamic.is_empty()).then_some(dynamic),
    })
}

fn benchmark_root_entity(
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
        id: ROOT_ID.to_string(),
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

fn benchmark_entity(
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
        Some(Term::Literal(literal)) => {
            let value = literal.value();
            if let Ok(boolean) = value.parse::<bool>() {
                EntityValue::EntityBool(boolean)
            } else if let Ok(integer) = value.parse::<i64>() {
                EntityValue::Entityi64(integer)
            } else if let Ok(float) = value.parse::<f64>() {
                EntityValue::Entityf64(float)
            } else {
                EntityValue::EntityString(value.to_string())
            }
        }
        _ => EntityValue::EntityString(term.0.clone()),
    }
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

fn encoded_subject(term: &EncodedTerm) -> Result<NamedOrBlankNode, RoCrateError> {
    match term.to_term() {
        Some(Term::NamedNode(node)) => Ok(NamedOrBlankNode::NamedNode(node)),
        Some(Term::BlankNode(node)) => Ok(NamedOrBlankNode::BlankNode(node)),
        Some(other) => Err(RoCrateError::InvalidGraph(format!(
            "subject must be a named or blank node, found `{other}`"
        ))),
        None => Err(RoCrateError::InvalidGraph(format!(
            "failed to parse subject `{}`",
            term.0
        ))),
    }
}

fn encoded_predicate(term: &EncodedTerm) -> Result<NamedNode, RoCrateError> {
    term.to_named_node().ok_or_else(|| {
        RoCrateError::InvalidGraph(format!(
            "predicate must be a named node, found `{}`",
            term.0
        ))
    })
}

fn encoded_object(term: &EncodedTerm) -> Result<Term, RoCrateError> {
    term.to_term()
        .ok_or_else(|| RoCrateError::InvalidGraph(format!("failed to parse object `{}`", term.0)))
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
    if id == ROOT_ID
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

fn license_from_str(license: &str) -> License {
    if looks_like_identifier(license) {
        License::Id(Id::Id(license.to_string()))
    } else {
        License::Description(license.to_string())
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
