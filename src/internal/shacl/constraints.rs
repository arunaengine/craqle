use std::collections::HashMap;

use crate::Result;
use crate::query_context::ReadContext;
use crate::rdf_read::RdfReadView;
use crate::store::TermId;

use super::term_meta::TermMeta;

#[derive(Default)]
pub(crate) struct TermMetaCache {
    values: HashMap<TermId, TermMeta>,
}

impl TermMetaCache {
    pub(crate) fn get<V: RdfReadView + ?Sized>(
        &mut self,
        view: &V,
        context: &ReadContext<'_>,
        term: TermId,
    ) -> Result<Option<&TermMeta>> {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.values.entry(term) {
            let encoded = view.decode_term(context, term)?;
            let Some(meta) = TermMeta::from_encoded(&encoded) else {
                return Ok(None);
            };
            entry.insert(meta);
        }
        Ok(self.values.get(&term))
    }
}

pub(crate) fn language_matches(language: &str, ranges: &[String]) -> bool {
    let language = language.to_ascii_lowercase();
    ranges.iter().any(|range| {
        range == "*"
            || language == *range
            || language
                .strip_prefix(range)
                .is_some_and(|suffix| suffix.starts_with('-'))
    })
}
