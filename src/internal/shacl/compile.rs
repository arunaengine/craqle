use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ::shacl::ir::{IRComponent, IRSchema, IRShape, ShapeLabelIdx};
use ::shacl::rdf::ShaclParser;
use ::shacl::types::{NodeKind, Severity, Target};
use oxrdf::graph::CanonicalizationAlgorithm;
use oxrdf::{BlankNode, Graph, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use rudof_iri::IriS;
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_core::SHACLPath;
use rudof_rdf::rdf_core::term::Object;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};

use crate::shacl::{CompiledShaclSchema, ShaclCompileOptions, ShaclCompileStatistics, ShaclError};
use crate::store::GraphStore;
use crate::{EncodedTerm, GraphId, GraphReplicaSnapshot, Result, RoCrateVersion};

use super::dependencies;
use super::model::{
    COMPILED_SHACL_FORMAT_VERSION, CompiledSchemaInner, CompiledShape, ConstraintPlan, MessagePlan,
    NodeKindPlan, PathPlan, SeverityPlan, ShapeId, ShapeKind, TargetPlan,
};

const CACHE_CAPACITY: usize = 32;
const EXTENSION_PROFILE: u32 = 0;
const RUDOF_VERSION: &str = "0.3.8";
const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const OWL_IMPORTS: &str = "<http://www.w3.org/2002/07/owl#imports>";
const SH_CONSTRAINT_COMPONENT: &str = "<http://www.w3.org/ns/shacl#ConstraintComponent>";
const SH_PROPERTY_SHAPE: &str = "<http://www.w3.org/ns/shacl#PropertyShape>";
const SH_PROPERTY: &str = "<http://www.w3.org/ns/shacl#property>";
const SH_TARGET_TYPE: &str = "<http://www.w3.org/ns/shacl#TargetType>";
const SH_PATH: &str = "<http://www.w3.org/ns/shacl#path>";
const SH_SPARQL: &str = "<http://www.w3.org/ns/shacl#sparql>";
const SH_RULE: &str = "<http://www.w3.org/ns/shacl#rule>";
const SH_EXPRESSION: &str = "<http://www.w3.org/ns/shacl#expression>";
const SH_JS_PREFIX: &str = "<http://www.w3.org/ns/shacl#js";

struct MaterializedShapes {
    digest: [u8; 32],
    ntriples: String,
    graph_count: usize,
    triple_count: usize,
}

fn materialize_shapes(
    store: &GraphStore,
    root: &GraphId,
    options: &ShaclCompileOptions,
) -> Result<MaterializedShapes> {
    let mut graphs = BTreeMap::new();
    let mut stack = Vec::new();
    visit_shape_graph(store, root, options, &mut graphs, &mut stack)?;

    let mut graph_union = Graph::new();
    let mut property_shapes = BTreeSet::new();
    let mut path_counts = BTreeMap::new();
    for (graph, snapshot) in &graphs {
        let scope_hash = blake3::hash(graph.as_bytes()).to_hex();
        let scope = &scope_hash.as_str()[..16];
        for quad in &snapshot.quads {
            let encoded_subject = scoped_term(&quad.subject, scope);
            let encoded_object = scoped_term(&quad.object, scope);
            if quad.predicate.0 == RDF_TYPE && quad.object.0 == SH_PROPERTY_SHAPE {
                property_shapes.insert(encoded_subject.clone());
            } else if quad.predicate.0 == SH_PROPERTY {
                property_shapes.insert(encoded_object.clone());
            }
            if quad.predicate.0 == SH_PATH {
                *path_counts.entry(encoded_subject.clone()).or_insert(0usize) += 1;
            }

            let subject = encoded_subject
                .to_term()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.subject))?;
            let subject = NamedOrBlankNode::try_from(subject)
                .map_err(|_| ill_formed_term(snapshot, &quad.subject))?;
            let predicate = quad
                .predicate
                .to_named_node()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.predicate))?;
            let object = encoded_object
                .to_term()
                .ok_or_else(|| ill_formed_term(snapshot, &quad.object))?;
            graph_union.insert(&Triple::new(subject, predicate, object));
        }
    }
    if let Some(shape) = property_shapes
        .iter()
        .find(|shape| path_counts.get(*shape).copied() != Some(1))
    {
        return Err(ShaclError::IllFormedShapes {
            graph: root.to_string(),
            message: format!("property shape {} must have exactly one sh:path", shape.0),
        }
        .into());
    }
    graph_union.canonicalize(CanonicalizationAlgorithm::Unstable);
    let mut triples = graph_union
        .iter()
        .map(|triple| triple.to_string())
        .collect::<Vec<_>>();
    triples.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"craqle-shacl-schema/v1\0");
    let mut ntriples = String::new();
    for triple in &triples {
        hash_field(&mut hasher, triple.as_bytes());
        ntriples.push_str(triple);
        ntriples.push_str(" .\n");
    }
    Ok(MaterializedShapes {
        digest: *hasher.finalize().as_bytes(),
        ntriples,
        graph_count: graphs.len(),
        triple_count: triples.len(),
    })
}

fn ill_formed_term(snapshot: &GraphReplicaSnapshot, term: &EncodedTerm) -> ShaclError {
    ShaclError::IllFormedShapes {
        graph: snapshot.graph.to_string(),
        message: format!("invalid RDF term {}", term.0),
    }
}

fn visit_shape_graph(
    store: &GraphStore,
    graph: &GraphId,
    options: &ShaclCompileOptions,
    graphs: &mut BTreeMap<String, GraphReplicaSnapshot>,
    stack: &mut Vec<String>,
) -> Result<()> {
    if let Some(cycle_start) = stack
        .iter()
        .position(|candidate| candidate == graph.as_str())
    {
        let mut cycle = stack[cycle_start..].to_vec();
        cycle.push(graph.to_string());
        return Err(ShaclError::ImportCycle { graphs: cycle }.into());
    }
    if graphs.contains_key(graph.as_str()) {
        return Ok(());
    }
    if !store.contains_graph(graph)? {
        return Err(ShaclError::ShapesGraphNotFound {
            graph: graph.to_string(),
        }
        .into());
    }

    stack.push(graph.to_string());
    let snapshot = store.graph_snapshot(graph)?;
    reject_unsupported_raw(&snapshot)?;
    let imports = imports(&snapshot)?;
    for import in imports {
        if !options.allow_local_imports {
            return Err(ShaclError::ImportsDisabled {
                graph: graph.to_string(),
                import,
            }
            .into());
        }
        let imported = GraphId::new(&import);
        if !store.contains_graph(&imported)? {
            return Err(ShaclError::ImportNotLocal {
                graph: graph.to_string(),
                import,
            }
            .into());
        }
        visit_shape_graph(store, &imported, options, graphs, stack)?;
    }
    stack.pop();
    graphs.insert(graph.to_string(), snapshot);
    Ok(())
}

fn imports(snapshot: &GraphReplicaSnapshot) -> Result<Vec<String>> {
    let mut imports = Vec::new();
    for quad in &snapshot.quads {
        if quad.predicate.0 != OWL_IMPORTS {
            continue;
        }
        let Some(import) = named_iri(&quad.object) else {
            return Err(ShaclError::IllFormedShapes {
                graph: snapshot.graph.to_string(),
                message: format!("owl:imports object must be an IRI, got {}", quad.object.0),
            }
            .into());
        };
        imports.push(import.to_owned());
    }
    imports.sort();
    imports.dedup();
    Ok(imports)
}

fn reject_unsupported_raw(snapshot: &GraphReplicaSnapshot) -> Result<()> {
    for quad in &snapshot.quads {
        if quad.subject.0.starts_with("<<") || quad.object.0.starts_with("<<") {
            let term = if quad.subject.0.starts_with("<<") {
                &quad.subject
            } else {
                &quad.object
            };
            return Err(ShaclError::UnsupportedRdfStarTerm {
                term: term.0.clone(),
            }
            .into());
        }
        let component = if quad.predicate.0 == SH_SPARQL {
            Some("http://www.w3.org/ns/shacl#SPARQLConstraintComponent")
        } else if quad.predicate.0 == SH_RULE {
            Some("http://www.w3.org/ns/shacl#rule")
        } else if quad.predicate.0 == SH_EXPRESSION {
            Some("http://www.w3.org/ns/shacl#expression")
        } else if quad.predicate.0.starts_with(SH_JS_PREFIX) {
            Some("http://www.w3.org/ns/shacl#JSConstraint")
        } else if quad.predicate.0 == RDF_TYPE
            && (quad.object.0 == SH_CONSTRAINT_COMPONENT || quad.object.0 == SH_TARGET_TYPE)
        {
            named_iri(&quad.object)
        } else {
            None
        };
        if let Some(component) = component {
            return Err(ShaclError::UnsupportedComponent {
                shape: quad.subject.0.clone(),
                component: component.to_owned(),
            }
            .into());
        }
    }
    Ok(())
}

fn named_iri(term: &EncodedTerm) -> Option<&str> {
    term.0.strip_prefix('<')?.strip_suffix('>')
}

fn scoped_term(term: &EncodedTerm, scope: &str) -> EncodedTerm {
    match term.0.strip_prefix("_:") {
        Some(label) => EncodedTerm(format!("_:g{scope}_{label}")),
        None => term.clone(),
    }
}

fn hash_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}
