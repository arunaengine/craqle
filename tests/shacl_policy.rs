#![cfg(feature = "shacl-core")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    Action, ActorId, AllowAllAuthorizer, AuthorizationError, CraqleError, CraqleNode,
    CraqleOptions, DenyAllAuthorizer, EncodedTerm, GrantAuthorizer, GraphId, GraphPolicy,
    MaterializedQuadChange, PermissionGrant, PermissionLevel, ShaclBinding, ShaclBindingOptions,
    ShaclCompileOptions, ShaclValidationOptions, ShaclValidationState, ValidationPolicy,
};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROP: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const SH_TARGET: &str = "http://www.w3.org/ns/shacl#targetNode";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_MIN: &str = "http://www.w3.org/ns/shacl#minCount";
const SH_MAX: &str = "http://www.w3.org/ns/shacl#maxCount";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const FOCUS: &str = "urn:test:shacl-policy:secret-focus";
const COMMON: &str = "urn:test:shacl-policy:common";

fn node() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x7a; 32])),
    )
    .unwrap();
    (directory, node)
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn number(value: u8) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\"^^<{XSD_INTEGER}>"))
}

fn add(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

fn del(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

fn policy(path: &str) -> GraphPolicy {
    GraphPolicy {
        public: false,
        permission_paths: vec![path.to_string()],
    }
}

fn grants(values: &[(&str, PermissionLevel)]) -> GrantAuthorizer {
    GrantAuthorizer::new(
        values
            .iter()
            .map(|(path, level)| PermissionGrant::new(*path, *level))
            .collect(),
    )
}

fn binding(data: &GraphId, shapes: &GraphId, policy: ValidationPolicy) -> ShaclBinding {
    ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy,
        validation_options: ShaclBindingOptions {
            allow_local_imports: true,
            ..ShaclBindingOptions::default()
        },
    }
}

fn shared_shape(node: &CraqleNode, graph: &GraphId) {
    node.apply_changes_unchecked(
        graph,
        vec![
            add(
                graph,
                "urn:test:shacl-policy:shared",
                RDF_TYPE,
                iri(SH_NODE),
            ),
            add(graph, "urn:test:shacl-policy:shared", SH_TARGET, iri(FOCUS)),
            add(
                graph,
                "urn:test:shacl-policy:shared",
                SH_PROPERTY,
                iri("urn:test:shacl-policy:shared-property"),
            ),
            add(
                graph,
                "urn:test:shacl-policy:shared-property",
                RDF_TYPE,
                iri(SH_PROP),
            ),
            add(
                graph,
                "urn:test:shacl-policy:shared-property",
                SH_PATH,
                iri(COMMON),
            ),
            add(
                graph,
                "urn:test:shacl-policy:shared-property",
                SH_MIN,
                number(1),
            ),
        ],
    )
    .unwrap();
}

fn shape_root(node: &CraqleNode, root: &GraphId, shared: &GraphId, name: &str, path: &str) {
    let shape = format!("urn:test:shacl-policy:{name}:shape");
    let property = format!("urn:test:shacl-policy:{name}:property");
    node.apply_changes_unchecked(
        root,
        vec![
            add(
                root,
                "urn:test:shacl-policy:ontology",
                OWL_IMPORTS,
                iri(shared.as_str()),
            ),
            add(root, &shape, RDF_TYPE, iri(SH_NODE)),
            add(root, &shape, SH_TARGET, iri(FOCUS)),
            add(root, &shape, SH_PROPERTY, iri(&property)),
            add(root, &property, RDF_TYPE, iri(SH_PROP)),
            add(root, &property, SH_PATH, iri(path)),
            add(root, &property, SH_MAX, number(0)),
        ],
    )
    .unwrap();
}

#[test]
fn auth_blocks_leaks() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:shacl-policy:auth-data");
    let root = GraphId::new("urn:test:shacl-policy:auth-root");
    let imported = GraphId::new("urn:test:shacl-policy:auth-import");
    let secret = "urn:test:shacl-policy:secret-path";

    node.apply_changes_unchecked(
        &root,
        vec![add(
            &root,
            "urn:test:shacl-policy:auth-ontology",
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &imported,
        vec![
            add(
                &imported,
                "urn:test:shacl-policy:auth-shape",
                RDF_TYPE,
                iri(SH_NODE),
            ),
            add(
                &imported,
                "urn:test:shacl-policy:auth-shape",
                SH_TARGET,
                iri(FOCUS),
            ),
            add(
                &imported,
                "urn:test:shacl-policy:auth-shape",
                SH_PROPERTY,
                iri("urn:test:shacl-policy:auth-property"),
            ),
            add(
                &imported,
                "urn:test:shacl-policy:auth-property",
                RDF_TYPE,
                iri(SH_PROP),
            ),
            add(
                &imported,
                "urn:test:shacl-policy:auth-property",
                SH_PATH,
                iri(secret),
            ),
            add(
                &imported,
                "urn:test:shacl-policy:auth-property",
                SH_MIN,
                number(1),
            ),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            "urn:test:shacl-policy:seed",
            "urn:test:shacl-policy:value",
            iri("urn:test:shacl-policy:seed-value"),
        )],
    )
    .unwrap();
    node.import_graph_policy(&data, policy("/data")).unwrap();
    node.import_graph_policy(&root, policy("/root")).unwrap();
    node.import_graph_policy(&imported, policy("/import"))
        .unwrap();

    let data_read = grants(&[("/data", PermissionLevel::Read)]);
    let data_write = grants(&[("/data", PermissionLevel::Write)]);
    let root_read = grants(&[("/root", PermissionLevel::Read)]);
    let data_root = grants(&[
        ("/data", PermissionLevel::Write),
        ("/root", PermissionLevel::Read),
    ]);
    let shape_read = grants(&[
        ("/root", PermissionLevel::Read),
        ("/import", PermissionLevel::Read),
    ]);
    let full = grants(&[
        ("/data", PermissionLevel::Write),
        ("/root", PermissionLevel::Read),
        ("/import", PermissionLevel::Read),
    ]);
    let bound = binding(&data, &root, ValidationPolicy::Enforce);
    let split = |graph: &GraphId, _: &GraphPolicy, action: Action| {
        if (graph == &data && action == Action::Write)
            || ((graph == &root || graph == &imported) && action == Action::Read)
        {
            return Ok(());
        }
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.to_string(),
        })
    };

    assert!(matches!(
        node.bind_shacl(&root_read, &bound),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Write,
                ..
            }
        ))
    ));
    assert!(matches!(
        node.bind_shacl(&data_write, &bound),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                ..
            }
        ))
    ));
    assert!(matches!(
        node.bind_shacl(&data_root, &bound),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                ..
            }
        ))
    ));
    let error = node.bind_shacl(&split, &bound).unwrap_err();
    assert!(matches!(
        &error,
        CraqleError::Authorization(AuthorizationError::PermissionDenied {
            action: Action::Read,
            ..
        })
    ));
    assert!(!error.to_string().contains(FOCUS));
    assert!(!error.to_string().contains(secret));
    let disabled = binding(&data, &root, ValidationPolicy::Disabled);
    let status = node.bind_shacl(&split, &disabled).unwrap();
    assert_eq!(status.state, ShaclValidationState::Pending);
    assert!(status.report.is_none());

    assert!(matches!(
        node.compile_shacl(
            &DenyAllAuthorizer,
            &root,
            &ShaclCompileOptions {
                allow_local_imports: true,
                ..ShaclCompileOptions::default()
            }
        ),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                ..
            }
        ))
    ));
    assert!(matches!(
        node.compile_shacl(
            &data_root,
            &root,
            &ShaclCompileOptions {
                allow_local_imports: true,
                ..ShaclCompileOptions::default()
            }
        ),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                ..
            }
        ))
    ));

    assert_eq!(
        node.bind_shacl(&full, &bound).unwrap().state,
        ShaclValidationState::Invalid
    );
    let schema = node
        .compile_shacl(
            &full,
            &root,
            &ShaclCompileOptions {
                allow_local_imports: true,
                ..ShaclCompileOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(
        node.validate_shacl(
            &shape_read,
            &data,
            &schema,
            &ShaclValidationOptions::default(),
        ),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                ..
            }
        ))
    ));

    let calls = Arc::new(Mutex::new(Vec::new()));
    let denied = {
        let calls = Arc::clone(&calls);
        move |graph: &GraphId, _: &GraphPolicy, action: Action| {
            calls.lock().unwrap().push((graph.to_string(), action));
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.to_string(),
            })
        }
    };
    let error = node.shacl_binding_statuses(&denied, &data).unwrap_err();
    assert!(matches!(
        &error,
        CraqleError::Authorization(AuthorizationError::PermissionDenied {
            action: Action::Read,
            ..
        })
    ));
    assert!(!error.to_string().contains(FOCUS));
    assert!(!error.to_string().contains(secret));
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(data.to_string(), Action::Read)]
    );

    let error = node.shacl_binding_statuses(&data_root, &data).unwrap_err();
    assert!(!error.to_string().contains(FOCUS));
    assert!(!error.to_string().contains(secret));
    let statuses = node.shacl_binding_statuses(&full, &data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert!(!statuses[0].report.as_ref().unwrap().conforms);
    assert_eq!(node.shacl_bindings(&full, &data).unwrap().len(), 1);

    node.apply_changes_unchecked(
        &root,
        vec![del(
            &root,
            "urn:test:shacl-policy:auth-ontology",
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    )
    .unwrap();
    let statuses = node.shacl_binding_statuses(&data_root, &data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert!(matches!(
        statuses[0].state,
        ShaclValidationState::Pending | ShaclValidationState::Valid | ShaclValidationState::Failed
    ));
    let report = statuses[0]
        .report
        .as_ref()
        .map(|report| format!("{report:?}"))
        .unwrap_or_default();
    assert!(
        !statuses[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains(secret)
    );
    assert!(!report.contains(FOCUS));
    assert!(!report.contains(secret));

    assert!(matches!(
        node.unbind_shacl(&data_read, &data, &root),
        Err(CraqleError::Authorization(
            AuthorizationError::PermissionDenied {
                action: Action::Write,
                ..
            }
        ))
    ));
    node.unbind_shacl(&full, &data, &root).unwrap();
}

#[test]
fn shacl_policy_snapshot() {
    let (_directory, node) = node();
    let node = Arc::new(node);
    let data = GraphId::new("urn:test:shacl-policy:snapshot-data");
    let shapes = GraphId::new("urn:test:shacl-policy:snapshot-shapes");

    shared_shape(node.as_ref(), &shapes);
    node.import_graph_policy(
        &shapes,
        GraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            FOCUS,
            COMMON,
            iri("urn:test:shacl-policy:snapshot-value"),
        )],
    )
    .unwrap();
    node.import_graph_policy(&data, policy("/data")).unwrap();
    let schema = node
        .compile_shacl(
            &AllowAllAuthorizer,
            &shapes,
            &ShaclCompileOptions::default(),
        )
        .unwrap();

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let node_for_validation = Arc::clone(&node);
    let data_for_validation = data.clone();
    let validation = std::thread::spawn(move || {
        let first = AtomicBool::new(true);
        let release_rx = Mutex::new(release_rx);
        let auth = move |graph: &GraphId, policy: &GraphPolicy, action: Action| {
            if first.swap(false, Ordering::SeqCst) {
                reached_tx.send(graph.clone()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .expect("SHACL authorization was never released");
                return Ok(());
            }
            if policy.public {
                return Ok(());
            }
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.to_string(),
            })
        };
        node_for_validation
            .validate_shacl(
                &auth,
                &data_for_validation,
                &schema,
                &ShaclValidationOptions::default(),
            )
            .unwrap()
    });

    assert_eq!(
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("SHACL validation never reached authorization"),
        data
    );
    node.apply_changes_unchecked(
        &data,
        vec![del(
            &data,
            FOCUS,
            COMMON,
            iri("urn:test:shacl-policy:snapshot-value"),
        )],
    )
    .unwrap();
    node.import_graph_policy(
        &data,
        GraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
    )
    .unwrap();
    release_tx.send(()).unwrap();

    assert!(validation.join().unwrap().conforms);
}

#[test]
fn status_reads_release_binding_lock_before_authorization() {
    let (_directory, node) = node();
    let node = Arc::new(node);
    let data = GraphId::new("urn:test:shacl-policy:locked-data");
    let shapes = GraphId::new("urn:test:shacl-policy:locked-shapes");

    shared_shape(node.as_ref(), &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            FOCUS,
            COMMON,
            iri("urn:test:shacl-policy:locked-value"),
        )],
    )
    .unwrap();
    node.bind_shacl(
        &AllowAllAuthorizer,
        &binding(&data, &shapes, ValidationPolicy::Advisory),
    )
    .unwrap();

    for target in [data.clone(), shapes.clone()] {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let status_node = Arc::clone(&node);
        let status_data = data.clone();
        let status_target = target.clone();
        let status = std::thread::spawn(move || {
            let waiting = AtomicBool::new(true);
            let release_rx = Mutex::new(release_rx);
            let auth = move |graph: &GraphId, _: &GraphPolicy, _: Action| {
                if graph == &status_target && waiting.swap(false, Ordering::SeqCst) {
                    reached_tx.send(()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .expect("status authorization was never released");
                }
                Ok(())
            };
            status_node
                .shacl_binding_statuses(&auth, &status_data)
                .unwrap()
        });
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("status authorization was never reached");

        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let policy_node = Arc::clone(&node);
        let policy_target = target.clone();
        let update = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            policy_node
                .import_graph_policy(&policy_target, policy("/revoked"))
                .unwrap();
            done_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("policy update stayed blocked behind status authorization");
        update.join().unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(status.join().unwrap().len(), 1);
    }
}

#[test]
fn bindings_share_versions() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:shacl-policy:multi-data");
    let shared = GraphId::new("urn:test:shacl-policy:multi-import");
    let enforce_one = GraphId::new("urn:test:shacl-policy:multi-enforce-one");
    let enforce_two = GraphId::new("urn:test:shacl-policy:multi-enforce-two");
    let advisory_one = GraphId::new("urn:test:shacl-policy:multi-advisory-one");
    let advisory_two = GraphId::new("urn:test:shacl-policy:multi-advisory-two");
    let disabled = GraphId::new("urn:test:shacl-policy:multi-disabled");
    let enforce_one_path = "urn:test:shacl-policy:multi-enforce-one-path";
    let enforce_two_path = "urn:test:shacl-policy:multi-enforce-two-path";
    let advisory_one_path = "urn:test:shacl-policy:multi-advisory-one-path";
    let advisory_two_path = "urn:test:shacl-policy:multi-advisory-two-path";
    let disabled_path = "urn:test:shacl-policy:multi-disabled-path";

    shared_shape(&node, &shared);
    for (root, name, path) in [
        (&enforce_one, "enforce-one", enforce_one_path),
        (&enforce_two, "enforce-two", enforce_two_path),
        (&advisory_one, "advisory-one", advisory_one_path),
        (&advisory_two, "advisory-two", advisory_two_path),
        (&disabled, "disabled", disabled_path),
    ] {
        shape_root(&node, root, &shared, name, path);
    }
    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            FOCUS,
            COMMON,
            iri("urn:test:shacl-policy:common-value"),
        )],
    )
    .unwrap();
    for (root, policy) in [
        (&enforce_one, ValidationPolicy::Enforce),
        (&enforce_two, ValidationPolicy::Enforce),
        (&advisory_one, ValidationPolicy::Advisory),
        (&advisory_two, ValidationPolicy::Advisory),
        (&disabled, ValidationPolicy::Disabled),
    ] {
        node.bind_shacl(&AllowAllAuthorizer, &binding(&data, root, policy))
            .unwrap();
    }

    let initial = node
        .shacl_binding_statuses(&AllowAllAuthorizer, &data)
        .unwrap()[0]
        .data_version;
    let before = node.graph_snapshot(&data).unwrap();
    for path in [enforce_one_path, enforce_two_path] {
        assert!(
            node.apply_changes(
                &AllowAllAuthorizer,
                &data,
                vec![add(
                    &data,
                    FOCUS,
                    path,
                    iri("urn:test:shacl-policy:blocked"),
                )],
            )
            .is_err()
        );
        assert_eq!(node.graph_snapshot(&data).unwrap(), before);
    }
    assert!(
        node.apply_changes(
            &AllowAllAuthorizer,
            &data,
            vec![del(
                &data,
                FOCUS,
                COMMON,
                iri("urn:test:shacl-policy:common-value"),
            )],
        )
        .is_err()
    );
    assert_eq!(node.graph_snapshot(&data).unwrap(), before);

    node.apply_changes(
        &AllowAllAuthorizer,
        &data,
        vec![
            add(
                &data,
                FOCUS,
                advisory_one_path,
                iri("urn:test:shacl-policy:advisory-one"),
            ),
            add(
                &data,
                FOCUS,
                advisory_two_path,
                iri("urn:test:shacl-policy:advisory-two"),
            ),
            add(
                &data,
                FOCUS,
                disabled_path,
                iri("urn:test:shacl-policy:disabled"),
            ),
        ],
    )
    .unwrap();

    let statuses = node
        .shacl_binding_statuses(&AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 5);
    let version = statuses[0].data_version;
    assert_ne!(version, initial);
    assert!(statuses.iter().all(|status| status.data_version == version));
    for root in [&enforce_one, &enforce_two] {
        let status = statuses
            .iter()
            .find(|status| status.binding.shapes_graph == *root)
            .unwrap();
        assert_eq!(status.state, ShaclValidationState::Valid);
        assert!(status.report.as_ref().unwrap().conforms);
    }
    for root in [&advisory_one, &advisory_two] {
        let status = statuses
            .iter()
            .find(|status| status.binding.shapes_graph == *root)
            .unwrap();
        assert_eq!(status.state, ShaclValidationState::Invalid);
        assert!(!status.report.as_ref().unwrap().conforms);
    }
    let status = statuses
        .iter()
        .find(|status| status.binding.shapes_graph == disabled)
        .unwrap();
    assert_eq!(status.state, ShaclValidationState::Pending);
    assert!(status.report.is_none());

    let current = node.graph_snapshot(&data).unwrap();
    let shared_max = "urn:test:shacl-policy:shared-max";
    node.apply_changes_unchecked(
        &shared,
        vec![
            add(
                &shared,
                "urn:test:shacl-policy:shared",
                SH_PROPERTY,
                iri(shared_max),
            ),
            add(&shared, shared_max, RDF_TYPE, iri(SH_PROP)),
            add(&shared, shared_max, SH_PATH, iri(COMMON)),
            add(&shared, shared_max, SH_MAX, number(0)),
        ],
    )
    .unwrap();
    assert_eq!(node.graph_snapshot(&data).unwrap(), current);

    let statuses = node
        .shacl_binding_statuses(&AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 5);
    assert!(statuses.iter().all(|status| status.data_version == version));
    let common_path = iri(COMMON).0;
    for root in [&enforce_one, &enforce_two] {
        let report = statuses
            .iter()
            .find(|status| status.binding.shapes_graph == *root)
            .and_then(|status| {
                assert_eq!(status.state, ShaclValidationState::Invalid);
                status.report.as_ref()
            })
            .unwrap();
        assert_eq!(report.results.len(), 1);
        assert_eq!(
            report.results[0].result_path.as_deref(),
            Some(common_path.as_str())
        );
    }
    for (root, path) in [
        (&advisory_one, advisory_one_path),
        (&advisory_two, advisory_two_path),
    ] {
        let report = statuses
            .iter()
            .find(|status| status.binding.shapes_graph == *root)
            .and_then(|status| {
                assert_eq!(status.state, ShaclValidationState::Invalid);
                status.report.as_ref()
            })
            .unwrap();
        let path = iri(path).0;
        assert_eq!(report.results.len(), 2);
        assert!(
            report
                .results
                .iter()
                .any(|result| result.result_path.as_deref() == Some(common_path.as_str()))
        );
        assert!(
            report
                .results
                .iter()
                .any(|result| result.result_path.as_deref() == Some(path.as_str()))
        );
    }
    let status = statuses
        .iter()
        .find(|status| status.binding.shapes_graph == disabled)
        .unwrap();
    assert_eq!(status.state, ShaclValidationState::Pending);
    assert!(status.report.is_none());
}
