#![allow(dead_code)]
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

pub mod perf;
pub mod sim;

use craqle::*;
use oxrdf::{NamedNode, Term};

pub use perf::*;
#[allow(unused_imports)]
pub use sim::*;

pub trait TestWriteExt {
    fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch>;

    fn apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch>;

    fn import_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> craqle::Result<()>;

    fn delete_graph_unchecked(&self, graph: &GraphId) -> craqle::Result<()>;
}

impl TestWriteExt for CraqleNode {
    fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch> {
        apply_test_changes_bypassing_structural_rules(self, graph, changes)
    }

    fn apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> craqle::Result<Batch> {
        apply_test_changes_bypassing_structural_rules(self, graph, changes)
    }

    fn import_graph_policy(&self, graph: &GraphId, policy: GraphPolicy) -> craqle::Result<()> {
        self.set_graph_policy(&AllowAllAuthorizer, graph, policy)
    }

    fn delete_graph_unchecked(&self, graph: &GraphId) -> craqle::Result<()> {
        self.delete_graph(&AllowAllAuthorizer, graph)
    }
}

fn apply_test_changes_bypassing_structural_rules(
    node: &CraqleNode,
    graph: &GraphId,
    changes: Vec<MaterializedQuadChange>,
) -> craqle::Result<Batch> {
    let mut quads = node
        .graph_snapshot(graph)?
        .quads
        .into_iter()
        .map(|quad| (quad.subject, quad.predicate, quad.object))
        .collect::<BTreeSet<_>>();
    for change in changes {
        let (change_graph, subject, predicate, object, insert) = match change {
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
        assert_eq!(&change_graph, graph, "test change targets the wrong graph");
        if insert {
            quads.insert((subject, predicate, object));
        } else {
            quads.remove(&(subject, predicate, object));
        }
    }

    let document = expanded_jsonld(&quads);
    node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
        &AllowAllAuthorizer,
        graph.clone(),
        &document,
        node.graph_policy(graph)?,
        CraqleRequestDurability::Durable,
        None,
    )
}

fn expanded_jsonld(quads: &BTreeSet<(EncodedTerm, EncodedTerm, EncodedTerm)>) -> String {
    let context = if quads
        .iter()
        .any(|(_, _, object)| object.0 == "<https://w3id.org/ro/crate/1.1>")
    {
        "https://w3id.org/ro/crate/1.1/context"
    } else if quads
        .iter()
        .any(|(_, _, object)| object.0 == "<https://w3id.org/ro/crate/1.2>")
    {
        "https://w3id.org/ro/crate/1.2/context"
    } else {
        "https://w3id.org/ro/crate/1.3/context"
    };
    let mut nodes = BTreeMap::<String, BTreeMap<String, Vec<serde_json::Value>>>::new();
    for (subject, predicate, object) in quads {
        let subject = resource_id(subject);
        let predicate = predicate
            .0
            .strip_prefix('<')
            .and_then(|value| value.strip_suffix('>'))
            .expect("test predicate must be an IRI")
            .to_owned();
        nodes
            .entry(subject)
            .or_default()
            .entry(predicate)
            .or_default()
            .push(jsonld_object(object));
    }
    let graph = nodes
        .into_iter()
        .map(|(subject, properties)| {
            let mut node = serde_json::Map::new();
            node.insert("@id".to_owned(), serde_json::Value::String(subject));
            node.extend(
                properties
                    .into_iter()
                    .map(|(predicate, values)| (predicate, serde_json::Value::Array(values))),
            );
            serde_json::Value::Object(node)
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "@context": context,
        "@graph": graph
    })
    .to_string()
}

fn resource_id(term: &EncodedTerm) -> String {
    term.0
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .map(str::to_owned)
        .or_else(|| term.0.starts_with("_:").then(|| term.0.clone()))
        .expect("test resource must be an IRI or blank node")
}

fn jsonld_object(term: &EncodedTerm) -> serde_json::Value {
    if let Some(iri) = term
        .0
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
    {
        return serde_json::json!({ "@id": iri });
    }
    if term.0.starts_with("_:") {
        return serde_json::json!({ "@id": term.0 });
    }
    #[allow(unreachable_patterns)]
    match Term::from_str(&term.0).expect("test object must be an RDF term") {
        Term::Literal(literal) => {
            let mut value = serde_json::Map::new();
            value.insert(
                "@value".to_owned(),
                serde_json::Value::String(literal.value().to_owned()),
            );
            if let Some(language) = literal.language() {
                value.insert(
                    "@language".to_owned(),
                    serde_json::Value::String(language.to_owned()),
                );
            } else if literal.datatype() != oxrdf::vocab::xsd::STRING {
                value.insert(
                    "@type".to_owned(),
                    serde_json::Value::String(literal.datatype().as_str().to_owned()),
                );
            }
            serde_json::Value::Object(value)
        }
        Term::NamedNode(_) | Term::BlankNode(_) => unreachable!("resource terms returned above"),
        _ => panic!("RDF-star is unsupported in test fixtures"),
    }
}

/// Generous enough that a slow machine never trips it, short enough that a real
/// deadlock fails the run instead of hanging it.
pub const WATCHDOG_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

pub fn query_with_test_visibility<F>(
    node: &CraqleNode,
    visible: F,
    sparql: &str,
) -> craqle::Result<QueryResults>
where
    F: Fn(&GraphId) -> bool + Send + Sync,
{
    let auth = move |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
        if visible(graph) {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    };
    node.query(&auth, sparql)
}

pub fn query_with_test_planner<F>(
    node: &CraqleNode,
    visible: F,
    sparql: &str,
    optimize: bool,
) -> craqle::Result<QueryResults>
where
    F: Fn(&GraphId) -> bool + Send + Sync,
{
    let auth = move |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
        if visible(graph) {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    };
    let query = node.prepare_query(sparql)?;
    let mut options = QueryExecutionOptions::default();
    options.optimize = optimize;
    Ok(node.execute_prepared(&auth, &query, &options)?.results)
}

/// Run `body` on a detached thread and fail if it does not finish in time.
///
/// The branch these tests guard replaces one engine-wide lock with a two-level
/// hierarchy, and a lock-order regression there presents as a **hang**, not as a
/// failed assertion. Joining would inherit the hang, so the worker is left
/// detached: the test fails, the harness keeps going, and CI reports a defect
/// instead of burning a runner until the job timeout.
pub fn with_watchdog(label: &'static str, body: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
        let _ = done.send(outcome);
    });
    match finished.recv_timeout(WATCHDOG_TIMEOUT) {
        Ok(Ok(())) => {}
        // Re-raised here so a failing assertion still reports its own message.
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("{label} made no progress within {WATCHDOG_TIMEOUT:?}: suspected deadlock")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{label} ended without reporting an outcome")
        }
    }
}

pub fn create_test_crate(net: &sim::CraqleCluster, peer: usize, graph: &GraphId) {
    net.peer(peer)
        .create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Test Dataset",
                "A test dataset",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
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
                Some(license.to_string()),
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
        &AllowAllAuthorizer,
        graph,
        vec![(
            EncodedTerm::from_named_node(&graph.0),
            EncodedTerm::from_named_node(&vocab::schema_keywords()),
            literal_term(keyword),
        )],
    )
    .unwrap();
}

pub fn keyword_delete(peer: &CraqleNode, graph: &GraphId, keyword: &str) {
    peer.apply_changes(
        &AllowAllAuthorizer,
        graph,
        vec![MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&graph.0),
            predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
            object: literal_term(keyword),
        }],
    )
    .unwrap();
}

pub fn reindex_and_search(net: &sim::CraqleCluster, peer: usize, query: &str) -> Vec<String> {
    net.reindex_search().unwrap();
    net.peer(peer)
        .search(
            &GrantAuthorizer::default(),
            SearchRequest { query, limit: 10 },
        )
        .unwrap()
        .into_iter()
        .map(|hit| hit.subject_iri)
        .collect()
}
