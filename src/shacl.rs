//! Craqle-owned SHACL compilation types.

use std::sync::Arc;
use std::time::Duration;

use crate::RoCrateVersion;
use crate::shacl_impl::model::CompiledSchemaInner;

/// SHACL feature profile implemented by Craqle's native validator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ShaclProfile {
    CraqleFastV1,
}

/// Options used while compiling a shapes graph.
#[derive(Clone, Debug, Default)]
pub struct ShaclCompileOptions {
    pub rocrate_version: RoCrateVersion,
    pub allow_local_imports: bool,
}

/// Work performed by one shapes compilation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShaclCompileStatistics {
    pub cache_hit: bool,
    pub shape_graphs: usize,
    pub shape_triples: usize,
    pub parse_time: Duration,
    pub compile_time: Duration,
}

/// Immutable, portable SHACL plan produced through Rudof's parser.
#[derive(Clone, Debug)]
pub struct CompiledShaclSchema {
    pub(crate) inner: Arc<CompiledSchemaInner>,
    pub(crate) statistics: ShaclCompileStatistics,
}

impl CompiledShaclSchema {
    pub fn schema_hash(&self) -> [u8; 32] {
        self.inner.schema_hash
    }

    pub fn plan_fingerprint(&self) -> [u8; 32] {
        self.inner.plan_fingerprint()
    }

    pub fn model_version(&self) -> u32 {
        self.inner.format_version
    }

    pub fn rocrate_version(&self) -> RoCrateVersion {
        self.inner.rocrate_version
    }

    pub fn profile(&self) -> ShaclProfile {
        ShaclProfile::CraqleFastV1
    }

    pub fn shape_count(&self) -> usize {
        self.inner.shapes.len()
    }

    pub fn statistics(&self) -> &ShaclCompileStatistics {
        &self.statistics
    }
}

/// SHACL shape parsing and compilation failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ShaclError {
    #[error("SHACL shapes graph `{graph}` does not exist")]
    ShapesGraphNotFound { graph: String },
    #[error("ill-formed SHACL shapes in `{graph}`: {message}")]
    IllFormedShapes { graph: String, message: String },
    #[error("unsupported SHACL component `{component}` on shape `{shape}`")]
    UnsupportedComponent { shape: String, component: String },
    #[error("owl:imports is disabled for `{graph}` (requested `{import}`)")]
    ImportsDisabled { graph: String, import: String },
    #[error("owl:imports target `{import}` from `{graph}` is not a local Craqle graph")]
    ImportNotLocal { graph: String, import: String },
    #[error("owl:imports cycle: {graphs:?}")]
    ImportCycle { graphs: Vec<String> },
    #[error("RDF-star term is unsupported in SHACL Performance v1: {term}")]
    UnsupportedRdfStarTerm { term: String },
}
