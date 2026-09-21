//! Checks unsupported search behavior in builds without search.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[cfg(not(feature = "search"))]
#[test]
fn search_disabled_error() {
    use craqle::{
        AllowAllAuthorizer, CraqleErrorKind, CraqleNode, GraphSearchRequest, GraphSearchRun,
        SearchOptions, SearchRequest, SearchRun,
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

    // A disabled build reports Unsupported before an ended budget or empty request.
    let cancelled = SearchOptions::default();
    cancelled.cancellation.cancel();
    let request = || SearchRequest {
        query: "anything",
        limit: 0,
    };
    let runs = [
        node.search_with_options(
            &AllowAllAuthorizer,
            SearchRun {
                request: request(),
                options: &cancelled,
            },
        )
        .map(|_| ()),
        node.search_resources_with(
            &AllowAllAuthorizer,
            SearchRun {
                request: request(),
                options: &cancelled,
            },
        )
        .map(|_| ()),
        node.search_graphs_with(
            &AllowAllAuthorizer,
            GraphSearchRun {
                request: GraphSearchRequest {
                    graphs: &[],
                    query: "anything",
                    limit: 0,
                },
                options: &cancelled,
            },
        )
        .map(|_| ()),
    ];
    for run in runs {
        assert_eq!(run.unwrap_err().kind(), CraqleErrorKind::Unsupported);
    }
}
