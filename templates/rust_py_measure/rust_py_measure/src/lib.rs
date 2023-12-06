use pyo3::{PyResult, types::PyModule, Python};
// use pyultima::measure::MeasureWrapper;
use serde::Deserialize;
// use frtb_engine::measures::frtb_measure_vec;
use ultibi::{polars::{series::{Series, IntoSeries}, datatypes::UInt32Chunked}, PolarsResult};
use pyo3_polars::derive::polars_expr;

// #[pyfunction]
// fn frtb_measures() -> Vec<MeasureWrapper> {
//     let frtb_measures = frtb_measure_vec();

//     frtb_measures.into_iter()
//         .map(|m|{MeasureWrapper{_inner: m}})
//         .collect::<Vec<MeasureWrapper>>()

// }

#[derive(Deserialize)]
struct SomeKwargs {
    capitalize: bool,
}

#[polars_expr(output_type=Float64)]
fn hamming_distance(inputs: &[Series], kwargs: SomeKwargs) -> PolarsResult<Series> {
    let a = inputs[0].utf8()?;
    let b = inputs[1].utf8()?;
    let v: Vec<u32> = vec![666];
    let out = UInt32Chunked::from_vec("Чe", v);
    Ok(out.into_series())
}

// #[pymodule]
// #[pyo3(name = "frtb_pyengine")]
// fn frtb_ultibi_engine(_py: Python, m: &PyModule) -> PyResult<()> {
//     let _frtb_measures = frtb_measures();
//     m.add("_frtb_measures", _frtb_measures)?;
//     m.add_function(wrap_pyfunction!(frtb_measures, m)?)?;
//     Ok(())
// }
