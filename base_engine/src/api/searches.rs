use polars::{series::Series, prelude::{PolarsResult, Utf8NameSpaceImpl}};

pub fn filter_contains(srs: &Series, pat: &str) -> PolarsResult<Series> {
    let mask = srs.utf8()?.contains(pat)?;
    srs.filter(&mask)
}