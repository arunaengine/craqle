use craqle::{
    vocab, AllowAllAuthorizer, CraqleNode, CreateCrateOptions, CreateCrateRequest, EncodedTerm,
    GraphId, GraphPolicy, MaterializedQuadChange, RoCrateVersion,
};

fn crate_request(graph: GraphId) -> CreateCrateRequest {
    CreateCrateRequest::new(
        graph,
        "Rules version coverage",
        "Rules use each stored RO-Crate profile.",
        "2026-08-20",
        None,
        GraphPolicy::default(),
    )
}

fn drop_name(graph: &GraphId) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: EncodedTerm::from_named_node(&graph.0),
        predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
        object: EncodedTerm("\"Rules version coverage\"".to_string()),
    }
}

#[test]
fn rules_match_versions() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let mut expected = None;

    for version in [
        RoCrateVersion::V1_1,
        RoCrateVersion::V1_2,
        RoCrateVersion::V1_3,
    ] {
        let graph = GraphId::new(&format!("urn:test:rules-version:{version:?}"));
        node.create_crate_with_options(
            &AllowAllAuthorizer,
            crate_request(graph.clone()),
            CreateCrateOptions {
                version,
                license: None,
            },
        )
        .unwrap();
        assert_eq!(node.crate_version(&graph).unwrap(), version);

        node.apply_changes_unchecked(&graph, vec![drop_name(&graph)])
            .unwrap();
        let codes: Vec<_> = node
            .graph_violations(&graph)
            .unwrap()
            .into_iter()
            .map(|violation| violation.code)
            .collect();
        assert_eq!(codes, ["missing_required_property"]);
        if let Some(expected) = &expected {
            assert_eq!(&codes, expected);
        } else {
            expected = Some(codes);
        }
    }
}
