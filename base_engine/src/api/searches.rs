use polars::{
    prelude::{PolarsResult, Utf8NameSpaceImpl},
    series::Series,
};

pub fn filter_contains_unique(srs: &Series, pat: &str) -> PolarsResult<Series> {
    let mask = srs
        .utf8()?
        .to_lowercase()
        .contains(pat.to_lowercase().as_str())?;
    let filtered = srs.filter(&mask)?;
    // Stable in order to preserve the order for pagination
    filtered.unique_stable()
}
