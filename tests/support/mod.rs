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

    pub fn export_jsonld(&self, graph: &GraphId) -> craqle::Result<String> {
        self.node.export_rocrate(&GrantAuthorizer::default(), graph)
    }

    pub fn export_jsonld_summary(&self, graph: &GraphId) -> craqle::Result<String> {
        self.node
            .export_rocrate_summary(&GrantAuthorizer::default(), graph)
    }

    pub fn export_jsonld_page(
        &self,
        graph: &GraphId,
        offset: usize,
        limit: usize,
    ) -> craqle::Result<RoCratePage> {
        self.node
            .export_rocrate_page(&GrantAuthorizer::default(), graph, offset, limit)
    }

    pub fn export_jsonld_page_after(
        &self,
        graph: &GraphId,
        after_entity_id: Option<&str>,
        limit: usize,
    ) -> craqle::Result<RoCratePage> {
        self.node.export_rocrate_page_after(
            &GrantAuthorizer::default(),
            graph,
            after_entity_id,
            limit,
        )
    }

    pub fn import_jsonld(&self, graph: GraphId, jsonld: &str) -> craqle::Result<Batch> {
        self.node
            .apply_rocrate_document_with_policy(&self.writer, graph, jsonld, public_policy())
    }

    pub fn import_jsonld_checked(&self, graph: GraphId, jsonld: &str) -> craqle::Result<Batch> {
        self.node.apply_rocrate_document_checked_with_policy(
            &self.writer,
            graph,
            jsonld,
            public_policy(),
        )
    }

    pub fn bootstrap_jsonld_trusted(&self, graph: GraphId, jsonld: &str) -> craqle::Result<Batch> {
        self.node
            .bootstrap_rocrate_document(&self.writer, graph, jsonld, public_policy())
    }
}

pub fn binding_literal(term: &EncodedTerm) -> String {
    match term.to_term() {
        Some(oxrdf::Term::Literal(literal)) => literal.value().to_string(),
        Some(other) => panic!("expected literal binding, got {other}"),
        None => panic!("failed to decode binding {}", term.0),
    }
}

pub fn binding_i64(term: &EncodedTerm) -> i64 {
    binding_literal(term).parse::<i64>().unwrap()
}

pub fn solution_rows(results: QueryResults) -> Vec<std::collections::HashMap<String, EncodedTerm>> {
    match results {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solution bindings, got {other:?}"),
    }
}

pub fn graph_state(
    net: &sim::CraqleCluster,
    peer: usize,
    graph: &GraphId,
) -> BTreeSet<(String, String, String)> {
    net.peer(peer)
        .graph_snapshot(graph)
        .unwrap()
        .quads
        .into_iter()
        .map(|quad| (quad.subject.0, quad.predicate.0, quad.object.0))
        .collect()
}

pub fn graph_contains(
    net: &sim::CraqleCluster,
    peer: usize,
    graph: &GraphId,
    subject: &str,
) -> bool {
    graph_state(net, peer, graph)
        .iter()
        .any(|(s, _, _)| s.contains(subject))
}

pub fn violation_messages(net: &sim::CraqleCluster, peer: usize, graph: &GraphId) -> Vec<String> {
    net.peer(peer)
        .graph_violations(graph)
        .unwrap()
        .into_iter()
        .map(|violation| violation.to_string())
        .collect()
}

pub fn keyword_insert(peer: &CraqleNode, graph: &GraphId, keyword: &str) {
    peer.insert_quads(
        graph,
        vec![(
            EncodedTerm::from_named_node(&vocab::root_entity()),
            EncodedTerm::from_named_node(&vocab::schema_keywords()),
            literal_term(keyword),
        )],
    )
    .unwrap();
}

pub fn keyword_delete(peer: &CraqleNode, graph: &GraphId, keyword: &str) {
    peer.apply_changes(
        graph,
        vec![MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&vocab::root_entity()),
            predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
            object: literal_term(keyword),
        }],
    )
    .unwrap();
}

pub fn reindex_and_search(net: &sim::CraqleCluster, peer: usize, query: &str) -> Vec<String> {
    net.reindex_search().unwrap();
    net.peer(peer)
        .search(&GrantAuthorizer::default(), query, 10)
        .unwrap()
        .into_iter()
        .map(|hit| hit.subject_iri)
        .collect()
}
