#[cfg(not(feature = "search"))]
#[test]
fn search_disabled_error() {
    use craqle::{
        AllowAllAuthorizer, CraqleErrorKind, CraqleNode, GraphSearchRequest, SearchRequest,
    };

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    let search = node
        .search(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "anything",
                limit: 0,
            },
        )
        .unwrap_err();
    assert_eq!(search.kind(), CraqleErrorKind::Unsupported);

    let resources = node
        .search_resources(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "anything",
                limit: 10,
            },
        )
        .unwrap_err();
    assert_eq!(resources.kind(), CraqleErrorKind::Unsupported);

    let graphs = node
        .search_graphs(
            &AllowAllAuthorizer,
            GraphSearchRequest {
                graphs: &[],
                query: "anything",
                limit: 10,
            },
        )
        .unwrap_err();
    assert_eq!(graphs.kind(), CraqleErrorKind::Unsupported);
}
