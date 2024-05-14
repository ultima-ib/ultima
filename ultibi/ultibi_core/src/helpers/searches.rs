use polars::{prelude::StringNameSpaceImpl, series::Series};

use crate::errors::{UltiResult, UltimaErr};

/// Helper function - used for search within a column
/// Filters Series by `pat`
/// Returns unique values
/// TODO cache function
pub fn filter_contains_unique(srs: &Series, pat: &str) -> UltiResult<Series> {
    let mask = srs
        .str()?
        .to_lowercase()
        .contains(pat.to_lowercase().as_str(), false)?;
    let filtered = srs.filter(&mask)?;
    // Stable in order to preserve the order for pagination
    filtered.unique_stable().map_err(UltimaErr::Polars)
}
