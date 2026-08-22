#[cfg(not(feature = "shacl-core"))]
fn main() {
    eprintln!("this example requires the `shacl-core` feature");
}

#[cfg(feature = "shacl-core")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use craqle::{
        AllowAllAuthorizer, CraqleNode, CreateCrateRequest, EncodedTerm, GraphId, GraphPolicy,
        PrepareRoCrateOptions, PreparedCommitMode, QueryOptions, RoCratePolicyOptions,
        RoCrateVersion, ShaclBinding, ShaclBindingOptions, ShaclCompileOptions, ShaclWritePolicy,
        UpdateOptions,
    };

    let directory =
        std::env::temp_dir().join(format!("craqle-api-workflow-{}", std::process::id()));
    if directory.exists() {
        std::fs::remove_dir_all(&directory)?;
    }
    let node = CraqleNode::open(&directory)?;
    let auth = AllowAllAuthorizer;

    // Create a crate through the typed API.
    let created = GraphId::new("urn:example:created");
    node.create_crate(
        &auth,
        CreateCrateRequest::new(
            created.clone(),
            "Created crate",
            "Created through the typed API",
            "2026-08-22",
            None,
            GraphPolicy::default(),
        ),
    )?;

    // Import a complete document through the ordinary checked import path.
    let imported = GraphId::new("urn:example:imported");
    node.apply_rocrate_document_with_policy(
        &auth,
        imported.clone(),
        &rocrate_document(&imported, "Imported crate"),
        GraphPolicy::default(),
    )?;

    // Install and compile a small Craqle SHACL Core Subset v1 policy.
    let shapes = GraphId::new("urn:example:shapes");
    let shape = iri("urn:example:dataset-shape");
    let property = EncodedTerm("_:identifier-property".to_owned());
    node.apply_changes(
        &auth,
        &shapes,
        vec![
            add(
                &shapes,
                shape.clone(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                iri("http://www.w3.org/ns/shacl#NodeShape"),
            ),
            add(
                &shapes,
                shape.clone(),
                "http://www.w3.org/ns/shacl#targetClass",
                iri("http://schema.org/Dataset"),
            ),
            add(
                &shapes,
                shape,
                "http://www.w3.org/ns/shacl#property",
                property.clone(),
            ),
            add(
                &shapes,
                property.clone(),
                "http://www.w3.org/ns/shacl#path",
                iri("http://schema.org/identifier"),
            ),
            add(
                &shapes,
                property,
                "http://www.w3.org/ns/shacl#minCount",
                EncodedTerm("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned()),
            ),
        ],
    )?;
    let policy = node.compile_rocrate_policy(
        &auth,
        &shapes,
        &ShaclCompileOptions {
            rocrate_version: RoCrateVersion::V1_3,
            allow_local_imports: false,
        },
    )?;

    // Prepare once, evaluate the policy, then commit the same encoded candidate.
    let prepared_graph = GraphId::new("urn:example:prepared");
    let prepared = node.prepare_rocrate_document(
        &auth,
        &prepared_graph,
        &rocrate_document(&prepared_graph, "Prepared crate"),
        &PrepareRoCrateOptions::default(),
    )?;
    let report =
        node.evaluate_rocrate_policy(&auth, &prepared, &policy, &RoCratePolicyOptions::default())?;
    assert!(report.conforms && report.accepted_by_write_policy);
    node.commit_prepared_rocrate_document(
        &auth,
        prepared,
        Some(&policy),
        PreparedCommitMode::Enforce,
    )?;

    // Execute a bounded authorized query.
    let query = node.prepare_query(&format!(
        "SELECT ?name WHERE {{ GRAPH <{}> {{ <{}> <http://schema.org/name> ?name }} }}",
        prepared_graph.as_str(),
        prepared_graph.as_str()
    ))?;
    let mut query_options = QueryOptions::default();
    query_options.limits.max_result_rows = 10;
    query_options.limits.max_result_cells = 10;
    let _execution = node.execute_prepared(&auth, &query, &query_options)?;

    // Apply an authorized, bounded SPARQL update.
    let mut update_options = UpdateOptions::default();
    update_options.limits.max_changes = 10;
    node.apply_sparql_update_with_options(
        &auth,
        &format!(
            "INSERT DATA {{ GRAPH <{}> {{ <{}> <http://schema.org/keywords> \"example\" }} }}",
            created.as_str(),
            created.as_str()
        ),
        &update_options,
    )?;

    // Bind SHACL and read the persisted validation status.
    node.bind_shacl(
        &auth,
        &ShaclBinding {
            data_graph: prepared_graph.clone(),
            shapes_graph: shapes,
            policy: ShaclWritePolicy::Enforce,
            validation_options: ShaclBindingOptions::default(),
        },
    )?;
    let statuses = node.shacl_binding_statuses(&auth, &prepared_graph)?;

    println!(
        "bounded query completed; {} SHACL binding status record(s)",
        statuses.len()
    );
    drop(node);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[cfg(feature = "shacl-core")]
fn iri(value: &str) -> craqle::EncodedTerm {
    craqle::EncodedTerm(format!("<{value}>"))
}

#[cfg(feature = "shacl-core")]
fn add(
    graph: &craqle::GraphId,
    subject: craqle::EncodedTerm,
    predicate: &str,
    object: craqle::EncodedTerm,
) -> craqle::MaterializedQuadChange {
    craqle::MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject,
        predicate: iri(predicate),
        object,
    }
}

#[cfg(feature = "shacl-core")]
fn rocrate_document(graph: &craqle::GraphId, name: &str) -> String {
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.3/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
                "about": {"@id": graph.as_str()}
            },
            {
                "@id": graph.as_str(),
                "@type": "Dataset",
                "name": name,
                "description": "Compiled public API example",
                "datePublished": "2026-08-22",
                "identifier": "example-dataset"
            }
        ]
    })
    .to_string()
}
