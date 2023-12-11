use ultibi::{polars::{series::{Series, IntoSeries}, datatypes::Float64Chunked}, PolarsResult, CPM};
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Float64)]
fn hamming_distance(inputs: &[Series], kwargs: CPM) -> PolarsResult<Series> {
    let _a = inputs[0].i64()?;
    let _b = inputs[1].i64()?;
    let r = kwargs.get("result").map(String::as_str).unwrap_or("1").parse::<f64>().unwrap();
    let v: Vec<f64> = vec![r];
    let out = Float64Chunked::from_vec("Чe", v);
    Ok(out.into_series())
}