#![cfg(feature = "shacl-core")]

use craqle::{
    ActorId, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange,
    RoCrateVersion, SearchStorage, ShaclCompileOptions, ShaclError,
};

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const RDF_FIRST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
const RDF_REST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
const RDF_NIL: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
const SH: &str = "http://www.w3.org/ns/shacl#";

fn iri(value: &str) -> String {
    format!("<{value}>")
}

fn sh(local: &str) -> String {
    iri(&format!("{SH}{local}"))
}

fn node() -> (tempfile::TempDir, CraqleNode) {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        database.path(),
        CraqleOptions::new()
            .with_actor(ActorId::from_bytes([0x5A; 32]))
            .with_search_storage(SearchStorage::Memory),
    )
    .unwrap();
    (database, node)
}

fn insert(node: &CraqleNode, graph: &GraphId, triples: &[(&str, &str, &str)]) {
    node.apply_changes_unchecked(
        graph,
        triples
            .iter()
            .map(
                |(subject, predicate, object)| MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm((*subject).to_owned()),
                    predicate: EncodedTerm((*predicate).to_owned()),
                    object: EncodedTerm((*object).to_owned()),
                },
            )
            .collect(),
    )
    .unwrap();
}

fn remove(node: &CraqleNode, graph: &GraphId, subject: &str, predicate: &str, object: &str) {
    node.apply_changes_unchecked(
        graph,
        vec![MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: EncodedTerm(subject.to_owned()),
            predicate: EncodedTerm(predicate.to_owned()),
            object: EncodedTerm(object.to_owned()),
        }],
    )
    .unwrap();
}

#[test]
fn compile_shape_cache() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:compile");
    let shape = iri("urn:test:shape");
    let predicate = iri("urn:test:predicate");
    let node_shape = sh("NodeShape");
    let target_subjects = sh("targetSubjectsOf");
    let property = sh("property");
    let path = sh("path");
    let min_count = sh("minCount");
    insert(
        &node,
        &graph,
        &[
            (&shape, RDF_TYPE, &node_shape),
            (&shape, &target_subjects, &predicate),
            (&shape, &property, "_:property"),
            ("_:property", &path, &predicate),
            (
                "_:property",
                &min_count,
                "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ),
        ],
    );

    let first = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    let second = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    assert_eq!(first.shape_count(), 2);
    assert!(!first.statistics().cache_hit);
    assert!(second.statistics().cache_hit);
    assert_eq!(first.schema_hash(), second.schema_hash());
    assert_eq!(first.plan_fingerprint(), second.plan_fingerprint());

    let versioned = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions {
                rocrate_version: RoCrateVersion::V1_2,
                ..ShaclCompileOptions::default()
            },
        )
        .unwrap();
    assert!(!versioned.statistics().cache_hit);
    assert_eq!(versioned.schema_hash(), first.schema_hash());
    assert_ne!(versioned.plan_fingerprint(), first.plan_fingerprint());
    assert_eq!(versioned.rocrate_version(), RoCrateVersion::V1_2);

    let max_count = sh("maxCount");
    insert(
        &node,
        &graph,
        &[(
            "_:property",
            &max_count,
            "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        )],
    );
    let changed = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    assert!(!changed.statistics().cache_hit);
    assert_ne!(first.schema_hash(), changed.schema_hash());
}

#[test]
fn canonical_digest() {
    let (_database, node) = node();
    let first_graph = GraphId::new("urn:test:shacl:canonical:first");
    let second_graph = GraphId::new("urn:test:shacl:canonical:second");
    let node_shape = sh("NodeShape");
    let target_node = sh("targetNode");
    let class = sh("class");
    let focus = iri("urn:test:focus");
    let required_class = iri("urn:test:class");
    insert(
        &node,
        &first_graph,
        &[
            ("_:first", RDF_TYPE, &node_shape),
            ("_:first", &target_node, &focus),
            ("_:first", &class, &required_class),
        ],
    );
    insert(
        &node,
        &second_graph,
        &[
            ("_:second", RDF_TYPE, &node_shape),
            ("_:second", &target_node, &focus),
            ("_:second", &class, &required_class),
        ],
    );

    let first = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &first_graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    let second = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &second_graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    assert_eq!(first.schema_hash(), second.schema_hash());
    assert_eq!(first.plan_fingerprint(), second.plan_fingerprint());
    assert!(second.statistics().cache_hit);
}

#[test]
fn compile_path_variants() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:paths");
    let property_shape = sh("PropertyShape");
    let path = sh("path");
    let inverse_path = sh("inversePath");
    let alternative_path = sh("alternativePath");
    let p1 = iri("urn:test:p1");
    let p2 = iri("urn:test:p2");
    insert(
        &node,
        &graph,
        &[
            ("_:inverseShape", RDF_TYPE, &property_shape),
            ("_:inverseShape", &path, "_:inverse"),
            ("_:inverse", &inverse_path, &p1),
            ("_:sequenceShape", RDF_TYPE, &property_shape),
            ("_:sequenceShape", &path, "_:sequence"),
            ("_:sequence", RDF_FIRST, &p1),
            ("_:sequence", RDF_REST, "_:sequenceTail"),
            ("_:sequenceTail", RDF_FIRST, &p2),
            ("_:sequenceTail", RDF_REST, RDF_NIL),
            ("_:alternativeShape", RDF_TYPE, &property_shape),
            ("_:alternativeShape", &path, "_:alternative"),
            ("_:alternative", &alternative_path, "_:alternativeList"),
            ("_:alternativeList", RDF_FIRST, &p1),
            ("_:alternativeList", RDF_REST, "_:alternativeTail"),
            ("_:alternativeTail", RDF_FIRST, &p2),
            ("_:alternativeTail", RDF_REST, RDF_NIL),
        ],
    );

    let schema = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    assert_eq!(schema.shape_count(), 3);
}

#[test]
fn compile_logical_lists() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:logical");
    let root = iri("urn:test:logical-root");
    let left = iri("urn:test:logical-left");
    let right = iri("urn:test:logical-right");
    let node_shape = sh("NodeShape");
    let and = sh("and");
    insert(
        &node,
        &graph,
        &[
            (&root, RDF_TYPE, &node_shape),
            (&left, RDF_TYPE, &node_shape),
            (&right, RDF_TYPE, &node_shape),
            (&root, &and, "_:logicalList"),
            ("_:logicalList", RDF_FIRST, &left),
            ("_:logicalList", RDF_REST, "_:logicalTail"),
            ("_:logicalTail", RDF_FIRST, &right),
            ("_:logicalTail", RDF_REST, RDF_NIL),
        ],
    );

    let schema = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    assert_eq!(schema.shape_count(), 3);
}

#[test]
fn reject_sparql() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:sparql");
    let shape = iri("urn:test:sparql-shape");
    let node_shape = sh("NodeShape");
    let sparql = sh("sparql");
    insert(
        &node,
        &graph,
        &[
            (&shape, RDF_TYPE, &node_shape),
            (&shape, &sparql, "_:constraint"),
        ],
    );

    let error = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::UnsupportedComponent { component, .. })
            if component.ends_with("SPARQLConstraintComponent")
    ));
}

#[test]
fn reject_custom_constraints() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:custom-component");
    let component = iri("urn:test:custom-component");
    let constraint_component = sh("ConstraintComponent");
    insert(
        &node,
        &graph,
        &[(&component, RDF_TYPE, &constraint_component)],
    );

    let error = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::UnsupportedComponent { component, .. })
            if component.ends_with("ConstraintComponent")
    ));
}

#[test]
fn reject_pathless_shape() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:ill-formed");
    let shape = iri("urn:test:property-without-path");
    let property_shape = sh("PropertyShape");
    insert(&node, &graph, &[(&shape, RDF_TYPE, &property_shape)]);

    let error = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::IllFormedShapes { .. })
    ));
}

#[test]
fn reject_multiple_paths() {
    let (_database, node) = node();
    let graph = GraphId::new("urn:test:shacl:multiple-paths");
    let shape = iri("urn:test:property-with-multiple-paths");
    let property_shape = sh("PropertyShape");
    let path = sh("path");
    insert(
        &node,
        &graph,
        &[
            (&shape, RDF_TYPE, &property_shape),
            (&shape, &path, &iri("urn:test:first-path")),
            (&shape, &path, &iri("urn:test:second-path")),
        ],
    );

    let error = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &graph,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::IllFormedShapes { .. })
    ));
}

#[test]
fn local_import_completes_shape() {
    let (_database, node) = node();
    let root = GraphId::new("urn:test:shacl:split-property-root");
    let imported = GraphId::new("urn:test:shacl:split-property-import");
    let owl_imports = iri("http://www.w3.org/2002/07/owl#imports");
    let node_shape = iri("urn:test:split-node-shape");
    let property_shape = iri("urn:test:split-property-shape");
    let node_shape_type = sh("NodeShape");
    let property_shape_type = sh("PropertyShape");
    let property = sh("property");
    let path = sh("path");
    insert(
        &node,
        &root,
        &[
            (&node_shape, RDF_TYPE, &node_shape_type),
            (&node_shape, &property, &property_shape),
            (
                &iri("urn:test:split-ontology"),
                &owl_imports,
                &iri(imported.as_str()),
            ),
        ],
    );
    insert(
        &node,
        &imported,
        &[
            (&property_shape, RDF_TYPE, &property_shape_type),
            (&property_shape, &path, &iri("urn:test:split-predicate")),
        ],
    );

    let schema = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &root,
            &ShaclCompileOptions {
                allow_local_imports: true,
                ..ShaclCompileOptions::default()
            },
        )
        .unwrap();
    assert_eq!(schema.shape_count(), 2);
}

#[test]
fn local_import_cache() {
    let (_database, node) = node();
    let root = GraphId::new("urn:test:shacl:root");
    let imported = GraphId::new("urn:test:shacl:imported");
    let owl_imports = iri("http://www.w3.org/2002/07/owl#imports");
    let shape = iri("urn:test:imported-shape");
    let node_shape = sh("NodeShape");
    insert(&node, &imported, &[(&shape, RDF_TYPE, &node_shape)]);
    insert(
        &node,
        &root,
        &[(
            &iri("urn:test:ontology"),
            &owl_imports,
            &iri(imported.as_str()),
        )],
    );

    let disabled = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &root,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        disabled,
        CraqleError::Shacl(ShaclError::ImportsDisabled { .. })
    ));

    let options = ShaclCompileOptions {
        allow_local_imports: true,
        ..ShaclCompileOptions::default()
    };
    let compiled = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert_eq!(compiled.shape_count(), 1);
    assert_eq!(compiled.statistics().shape_graphs, 2);

    let second_shape = iri("urn:test:second-imported-shape");
    insert(&node, &imported, &[(&second_shape, RDF_TYPE, &node_shape)]);
    let changed = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert!(!changed.statistics().cache_hit);
    assert_eq!(changed.shape_count(), 2);
    assert_ne!(compiled.schema_hash(), changed.schema_hash());
}

#[test]
fn import_topology_fence() {
    let (_database, node) = node();
    let root = GraphId::new("urn:test:shacl:topology-root");
    let imported = GraphId::new("urn:test:shacl:topology-imported");
    let replacement = GraphId::new("urn:test:shacl:topology-replacement");
    let data = GraphId::new("urn:test:shacl:topology-data");
    let owl_imports = iri("http://www.w3.org/2002/07/owl#imports");
    let ontology = iri("urn:test:topology-ontology");
    let shape = iri("urn:test:topology-shape");
    let node_shape = sh("NodeShape");

    insert(&node, &imported, &[(&shape, RDF_TYPE, &node_shape)]);
    insert(
        &node,
        &root,
        &[(&ontology, &owl_imports, &iri(imported.as_str()))],
    );
    insert(
        &node,
        &data,
        &[(
            &iri("urn:test:topology-focus"),
            RDF_TYPE,
            &iri("urn:test:topology-type"),
        )],
    );

    let options = ShaclCompileOptions {
        allow_local_imports: true,
        ..ShaclCompileOptions::default()
    };
    let compiled = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert_eq!(compiled.statistics().shape_graphs, 2);
    assert!(!compiled.statistics().cache_hit);
    let cached = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert!(cached.statistics().cache_hit);
    assert_eq!(compiled.plan_fingerprint(), cached.plan_fingerprint());

    node.delete_graph_unchecked(&imported).unwrap();
    assert!(!node.contains_graph(&imported).unwrap());

    let missing = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap_err();
    assert!(matches!(
        missing,
        CraqleError::Shacl(ShaclError::ImportNotLocal { import, .. })
            if import == imported.as_str()
    ));
    let stale = node
        .validate_shacl(
            &craqle::AllowAllAuthorizer,
            &data,
            &compiled,
            &craqle::ShaclValidationOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        stale,
        CraqleError::Shacl(ShaclError::SchemaChangedDuringValidation { .. })
    ));

    insert(&node, &replacement, &[(&shape, RDF_TYPE, &node_shape)]);
    remove(
        &node,
        &root,
        &ontology,
        &owl_imports,
        &iri(imported.as_str()),
    );
    insert(
        &node,
        &root,
        &[(&ontology, &owl_imports, &iri(replacement.as_str()))],
    );
    let recreated = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert!(!recreated.statistics().cache_hit);
    assert_eq!(recreated.statistics().shape_graphs, 2);
    assert_ne!(compiled.plan_fingerprint(), recreated.plan_fingerprint());
    let recreated_cached = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert!(recreated_cached.statistics().cache_hit);
    assert_eq!(
        recreated.plan_fingerprint(),
        recreated_cached.plan_fingerprint()
    );
}

#[test]
fn imported_nodes_local() {
    let (_database, node) = node();
    let root = GraphId::new("urn:test:shacl:scoped-root");
    let first = GraphId::new("urn:test:shacl:scoped-first");
    let second = GraphId::new("urn:test:shacl:scoped-second");
    let owl_imports = iri("http://www.w3.org/2002/07/owl#imports");
    let node_shape = sh("NodeShape");
    insert(&node, &first, &[("_:shape", RDF_TYPE, &node_shape)]);
    insert(&node, &second, &[("_:shape", RDF_TYPE, &node_shape)]);
    insert(
        &node,
        &root,
        &[
            (
                &iri("urn:test:scoped-ontology"),
                &owl_imports,
                &iri(first.as_str()),
            ),
            (
                &iri("urn:test:scoped-ontology"),
                &owl_imports,
                &iri(second.as_str()),
            ),
        ],
    );
    let options = ShaclCompileOptions {
        allow_local_imports: true,
        ..ShaclCompileOptions::default()
    };

    let schema = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &root, &options)
        .unwrap();
    assert_eq!(schema.shape_count(), 2);
    assert_eq!(schema.statistics().shape_graphs, 3);
}

#[test]
fn reject_import_cycle() {
    let (_database, node) = node();
    let first = GraphId::new("urn:test:shacl:cycle:first");
    let second = GraphId::new("urn:test:shacl:cycle:second");
    let owl_imports = iri("http://www.w3.org/2002/07/owl#imports");
    insert(
        &node,
        &first,
        &[(&iri("urn:test:first"), &owl_imports, &iri(second.as_str()))],
    );
    insert(
        &node,
        &second,
        &[(&iri("urn:test:second"), &owl_imports, &iri(first.as_str()))],
    );
    let options = ShaclCompileOptions {
        allow_local_imports: true,
        ..ShaclCompileOptions::default()
    };

    let error = node
        .compile_shacl(&craqle::AllowAllAuthorizer, &first, &options)
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::ImportCycle { .. })
    ));
}
