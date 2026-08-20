use crate::{EncodedTerm, RoCrateVersion};

pub(crate) const COMPILED_SHACL_FORMAT_VERSION: u32 = 1;

#[derive(Debug)]
pub(crate) struct CompiledSchemaInner {
    pub(crate) format_version: u32,
    pub(crate) schema_hash: [u8; 32],
    pub(crate) rocrate_version: RoCrateVersion,
    pub(crate) shapes: Box<[CompiledShape]>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ShapeId(pub(crate) u32);

#[derive(Debug)]
pub(crate) struct CompiledShape {
    pub(crate) id: ShapeId,
    pub(crate) label: EncodedTerm,
    pub(crate) kind: ShapeKind,
    pub(crate) targets: Box<[TargetPlan]>,
    pub(crate) path: Option<PathPlan>,
    pub(crate) constraints: Box<[ConstraintPlan]>,
    pub(crate) property_shapes: Box<[ShapeId]>,
    pub(crate) severity: SeverityPlan,
    pub(crate) messages: Box<[MessagePlan]>,
    pub(crate) deactivated: bool,
    pub(crate) dependencies: ShapeDependencies,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShapeKind {
    Node,
    Property,
}

#[derive(Debug)]
pub(crate) enum TargetPlan {
    Node(EncodedTerm),
    Class(EncodedTerm),
    SubjectsOf(EncodedTerm),
    ObjectsOf(EncodedTerm),
    ImplicitClass(EncodedTerm),
}

#[derive(Debug)]
pub(crate) enum PathPlan {
    Predicate(EncodedTerm),
    Alternative(Box<[PathPlan]>),
    Sequence(Box<[PathPlan]>),
    Inverse(Box<PathPlan>),
    ZeroOrMore(Box<PathPlan>),
    OneOrMore(Box<PathPlan>),
    ZeroOrOne(Box<PathPlan>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeKindPlan {
    Iri,
    Literal,
    BlankNode,
    BlankNodeOrIri,
    BlankNodeOrLiteral,
    IriOrLiteral,
}

#[derive(Debug)]
pub(crate) enum ConstraintPlan {
    Class(EncodedTerm),
    Datatype(EncodedTerm),
    NodeKind(NodeKindPlan),
    MinCount(usize),
    MaxCount(usize),
    MinExclusive(EncodedTerm),
    MaxExclusive(EncodedTerm),
    MinInclusive(EncodedTerm),
    MaxInclusive(EncodedTerm),
    MinLength(usize),
    MaxLength(usize),
    Pattern {
        pattern: String,
        flags: Option<String>,
    },
    UniqueLang(bool),
    LanguageIn(Box<[String]>),
    Equals(EncodedTerm),
    Disjoint(EncodedTerm),
    LessThan(EncodedTerm),
    LessThanOrEquals(EncodedTerm),
    Or(Box<[ShapeId]>),
    And(Box<[ShapeId]>),
    Not(ShapeId),
    Xone(Box<[ShapeId]>),
    Node(ShapeId),
    HasValue(EncodedTerm),
    In(Box<[EncodedTerm]>),
    QualifiedValueShape {
        shape: ShapeId,
        min_count: Option<usize>,
        max_count: Option<usize>,
        disjoint: bool,
        siblings: Box<[ShapeId]>,
    },
    Closed {
        ignored_properties: Box<[EncodedTerm]>,
    },
}

#[derive(Debug)]
pub(crate) enum SeverityPlan {
    Trace,
    Debug,
    Info,
    Warning,
    Violation,
    Custom(EncodedTerm),
}

#[derive(Debug)]
pub(crate) struct MessagePlan {
    pub(crate) language: Option<String>,
    pub(crate) text: String,
}

#[derive(Debug, Default)]
pub(crate) struct ShapeDependencies {
    pub(crate) forward_predicates: Box<[EncodedTerm]>,
    pub(crate) inverse_predicates: Box<[EncodedTerm]>,
    pub(crate) target_classes: Box<[EncodedTerm]>,
    pub(crate) nested_shapes: Box<[ShapeId]>,
    pub(crate) reads_rdf_type: bool,
    pub(crate) reads_all_outgoing_predicates: bool,
    pub(crate) has_transitive_path: bool,
    pub(crate) requires_global_work: bool,
}

impl CompiledSchemaInner {
    pub(crate) fn plan_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"craqle-shacl-plan/v1\0");
        hash_u64(&mut hasher, u64::from(self.format_version));
        hasher.update(&self.schema_hash);
        hash_u64(
            &mut hasher,
            match self.rocrate_version {
                RoCrateVersion::V1_1 => 11,
                RoCrateVersion::V1_2 => 12,
                RoCrateVersion::V1_3 => 13,
            },
        );
        hash_u64(&mut hasher, self.shapes.len() as u64);
        for shape in &self.shapes {
            shape.hash_plan(&mut hasher);
        }
        *hasher.finalize().as_bytes()
    }
}

impl CompiledShape {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        hash_u64(hasher, u64::from(self.id.0));
        hash_term(hasher, &self.label);
        hash_u64(
            hasher,
            match self.kind {
                ShapeKind::Node => 0,
                ShapeKind::Property => 1,
            },
        );
        hash_u64(hasher, self.targets.len() as u64);
        for target in &self.targets {
            target.hash_plan(hasher);
        }
        hash_option(hasher, self.path.as_ref(), PathPlan::hash_plan);
        hash_u64(hasher, self.constraints.len() as u64);
        for constraint in &self.constraints {
            constraint.hash_plan(hasher);
        }
        hash_shape_ids(hasher, &self.property_shapes);
        self.severity.hash_plan(hasher);
        hash_u64(hasher, self.messages.len() as u64);
        for message in &self.messages {
            match &message.language {
                Some(language) => {
                    hash_u64(hasher, 1);
                    hash_bytes(hasher, language.as_bytes());
                }
                None => hash_u64(hasher, 0),
            }
            hash_bytes(hasher, message.text.as_bytes());
        }
        hash_bool(hasher, self.deactivated);
        self.dependencies.hash_plan(hasher);
    }
}

impl TargetPlan {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        let (tag, term) = match self {
            Self::Node(term) => (0, term),
            Self::Class(term) => (1, term),
            Self::SubjectsOf(term) => (2, term),
            Self::ObjectsOf(term) => (3, term),
            Self::ImplicitClass(term) => (4, term),
        };
        hash_u64(hasher, tag);
        hash_term(hasher, term);
    }
}

impl PathPlan {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Predicate(predicate) => {
                hash_u64(hasher, 0);
                hash_term(hasher, predicate);
            }
            Self::Alternative(paths) => {
                hash_u64(hasher, 1);
                hash_paths(hasher, paths);
            }
            Self::Sequence(paths) => {
                hash_u64(hasher, 2);
                hash_paths(hasher, paths);
            }
            Self::Inverse(path) => {
                hash_u64(hasher, 3);
                path.hash_plan(hasher);
            }
            Self::ZeroOrMore(path) => {
                hash_u64(hasher, 4);
                path.hash_plan(hasher);
            }
            Self::OneOrMore(path) => {
                hash_u64(hasher, 5);
                path.hash_plan(hasher);
            }
            Self::ZeroOrOne(path) => {
                hash_u64(hasher, 6);
                path.hash_plan(hasher);
            }
        }
    }
}

impl ConstraintPlan {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Class(term) => hash_tagged_term(hasher, 0, term),
            Self::Datatype(term) => hash_tagged_term(hasher, 1, term),
            Self::NodeKind(kind) => {
                hash_u64(hasher, 2);
                hash_u64(
                    hasher,
                    match kind {
                        NodeKindPlan::Iri => 0,
                        NodeKindPlan::Literal => 1,
                        NodeKindPlan::BlankNode => 2,
                        NodeKindPlan::BlankNodeOrIri => 3,
                        NodeKindPlan::BlankNodeOrLiteral => 4,
                        NodeKindPlan::IriOrLiteral => 5,
                    },
                );
            }
            Self::MinCount(value) => hash_tagged_usize(hasher, 3, *value),
            Self::MaxCount(value) => hash_tagged_usize(hasher, 4, *value),
            Self::MinExclusive(term) => hash_tagged_term(hasher, 5, term),
            Self::MaxExclusive(term) => hash_tagged_term(hasher, 6, term),
            Self::MinInclusive(term) => hash_tagged_term(hasher, 7, term),
            Self::MaxInclusive(term) => hash_tagged_term(hasher, 8, term),
            Self::MinLength(value) => hash_tagged_usize(hasher, 9, *value),
            Self::MaxLength(value) => hash_tagged_usize(hasher, 10, *value),
            Self::Pattern { pattern, flags } => {
                hash_u64(hasher, 11);
                hash_bytes(hasher, pattern.as_bytes());
                match flags {
                    Some(flags) => {
                        hash_u64(hasher, 1);
                        hash_bytes(hasher, flags.as_bytes());
                    }
                    None => hash_u64(hasher, 0),
                }
            }
            Self::UniqueLang(value) => {
                hash_u64(hasher, 12);
                hash_bool(hasher, *value);
            }
            Self::LanguageIn(languages) => {
                hash_u64(hasher, 13);
                hash_u64(hasher, languages.len() as u64);
                for language in languages {
                    hash_bytes(hasher, language.as_bytes());
                }
            }
            Self::Equals(term) => hash_tagged_term(hasher, 14, term),
            Self::Disjoint(term) => hash_tagged_term(hasher, 15, term),
            Self::LessThan(term) => hash_tagged_term(hasher, 16, term),
            Self::LessThanOrEquals(term) => hash_tagged_term(hasher, 17, term),
            Self::Or(shapes) => hash_tagged_shapes(hasher, 18, shapes),
            Self::And(shapes) => hash_tagged_shapes(hasher, 19, shapes),
            Self::Not(shape) => hash_tagged_shape(hasher, 20, *shape),
            Self::Xone(shapes) => hash_tagged_shapes(hasher, 21, shapes),
            Self::Node(shape) => hash_tagged_shape(hasher, 22, *shape),
            Self::HasValue(term) => hash_tagged_term(hasher, 23, term),
            Self::In(terms) => {
                hash_u64(hasher, 24);
                hash_terms(hasher, terms);
            }
            Self::QualifiedValueShape {
                shape,
                min_count,
                max_count,
                disjoint,
                siblings,
            } => {
                hash_u64(hasher, 25);
                hash_u64(hasher, u64::from(shape.0));
                hash_optional_usize(hasher, *min_count);
                hash_optional_usize(hasher, *max_count);
                hash_bool(hasher, *disjoint);
                hash_shape_ids(hasher, siblings);
            }
            Self::Closed { ignored_properties } => {
                hash_u64(hasher, 26);
                hash_terms(hasher, ignored_properties);
            }
        }
    }
}

impl SeverityPlan {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Trace => hash_u64(hasher, 0),
            Self::Debug => hash_u64(hasher, 1),
            Self::Info => hash_u64(hasher, 2),
            Self::Warning => hash_u64(hasher, 3),
            Self::Violation => hash_u64(hasher, 4),
            Self::Custom(term) => hash_tagged_term(hasher, 5, term),
        }
    }
}

impl ShapeDependencies {
    fn hash_plan(&self, hasher: &mut blake3::Hasher) {
        hash_terms(hasher, &self.forward_predicates);
        hash_terms(hasher, &self.inverse_predicates);
        hash_terms(hasher, &self.target_classes);
        hash_shape_ids(hasher, &self.nested_shapes);
        hash_bool(hasher, self.reads_rdf_type);
        hash_bool(hasher, self.reads_all_outgoing_predicates);
        hash_bool(hasher, self.has_transitive_path);
        hash_bool(hasher, self.requires_global_work);
    }
}

fn hash_paths(hasher: &mut blake3::Hasher, paths: &[PathPlan]) {
    hash_u64(hasher, paths.len() as u64);
    for path in paths {
        path.hash_plan(hasher);
    }
}

fn hash_terms(hasher: &mut blake3::Hasher, terms: &[EncodedTerm]) {
    hash_u64(hasher, terms.len() as u64);
    for term in terms {
        hash_term(hasher, term);
    }
}

fn hash_shape_ids(hasher: &mut blake3::Hasher, shapes: &[ShapeId]) {
    hash_u64(hasher, shapes.len() as u64);
    for shape in shapes {
        hash_u64(hasher, u64::from(shape.0));
    }
}

fn hash_option<T>(
    hasher: &mut blake3::Hasher,
    value: Option<&T>,
    hash: impl FnOnce(&T, &mut blake3::Hasher),
) {
    match value {
        Some(value) => {
            hash_u64(hasher, 1);
            hash(value, hasher);
        }
        None => hash_u64(hasher, 0),
    }
}

fn hash_optional_usize(hasher: &mut blake3::Hasher, value: Option<usize>) {
    match value {
        Some(value) => {
            hash_u64(hasher, 1);
            hash_u64(hasher, value as u64);
        }
        None => hash_u64(hasher, 0),
    }
}

fn hash_tagged_term(hasher: &mut blake3::Hasher, tag: u64, term: &EncodedTerm) {
    hash_u64(hasher, tag);
    hash_term(hasher, term);
}

fn hash_tagged_usize(hasher: &mut blake3::Hasher, tag: u64, value: usize) {
    hash_u64(hasher, tag);
    hash_u64(hasher, value as u64);
}

fn hash_tagged_shape(hasher: &mut blake3::Hasher, tag: u64, shape: ShapeId) {
    hash_u64(hasher, tag);
    hash_u64(hasher, u64::from(shape.0));
}

fn hash_tagged_shapes(hasher: &mut blake3::Hasher, tag: u64, shapes: &[ShapeId]) {
    hash_u64(hasher, tag);
    hash_shape_ids(hasher, shapes);
}

fn hash_term(hasher: &mut blake3::Hasher, term: &EncodedTerm) {
    hash_bytes(hasher, term.0.as_bytes());
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hash_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn hash_bool(hasher: &mut blake3::Hasher, value: bool) {
    hasher.update(&[u8::from(value)]);
}

fn hash_u64(hasher: &mut blake3::Hasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}
