#![allow(dead_code)]

use std::collections::BTreeSet;

pub mod perf;
pub mod sim;

use craqle::*;
use oxrdf::{NamedNode, Term};

pub use perf::*;
#[allow(unused_imports)]
pub use sim::*;

pub fn create_test_crate(net: &sim::CraqleCluster, peer: usize, graph: &GraphId) {
    net.peer(peer)
        .create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Test Dataset",
                "A test dataset",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();
}

pub struct TestRoCrateApi<'a> {
    node: &'a CraqleNode,
    writer: GrantAuthorizer,
}

pub fn manager(node: &CraqleNode) -> TestRoCrateApi<'_> {
    TestRoCrateApi {
        node,
        writer: writer_auth(),
    }
}

impl<'a> TestRoCrateApi<'a> {
    pub fn create_crate(
        &self,
        graph: GraphId,
        name: &str,
        description: &str,
        date_published: &str,
        license: &str,
    ) -> craqle::Result<Batch> {
        self.node.create_crate(
            &self.writer,
            CreateCrateRequest::new(
                graph,
                name,
                description,
                date_published,
                license,
                public_policy(),
            ),
        )
    }

    pub fn add_data_entity(
        &self,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> craqle::Result<Batch> {
        self.node.add_data_entity_with_triples(
            &self.writer,
            graph,
            entity_id,
            entity_type,
            name,
            additional_triples,
        )
    }

    pub fn add_data_entity_under(
        &self,
        graph: &GraphId,
        parent_id: &str,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> craqle::Result<AppendDataEntitiesReport> {
        self.node.append_new_data_entities_under(
            &self.writer,
            graph,
            parent_id,
            vec![NewDataEntity {
                entity_id: entity_id.to_string(),
                entity_type: entity_type.to_string(),
                name: name.to_string(),
                additional_triples,
            }],
        )
    }

    pub fn add_contextual_entity(
        &self,
        graph: &GraphId,
        entity_id: &str,
        entity_type: &str,
        name: &str,
        additional_triples: Vec<(NamedNode, Term)>,
    ) -> craqle::Result<Batch> {
        self.node.add_contextual_entity_with_triples(
            &self.writer,
            graph,
            entity_id,
            entity_type,
            name,
            additional_triples,
        )
    }

    pub fn update_property(
        &self,
        graph: &GraphId,
        entity_id: &str,
        predicate: &str,
        old_value: Option<&str>,
        new_value: &str,
    ) -> craqle::Result<Batch> {
        self.node.update_property(
            &self.writer,
            graph,
            entity_id,
            predicate,
            old_value,
            new_value,
        )
    }

