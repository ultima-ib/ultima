use pyo3::{PyResult, types::PyModule, Python};
// use pyultima::measure::MeasureWrapper;
use serde::Deserialize;
// use frtb_engine::measures::frtb_measure_vec;
use ultibi::{polars::{series::{Series, IntoSeries}, datatypes::Float64Chunked}, PolarsResult, CPM};
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Float64)]
fn hamming_distance(inputs: &[Series], kwargs: CPM) -> PolarsResult<Series> {
    let _a = inputs[0].i64()?;
    let _b = inputs[1].i64()?;
    let v: Vec<f64> = vec![777.0];
    let out = Float64Chunked::from_vec("Чe", v);
    Ok(out.into_series())
}
