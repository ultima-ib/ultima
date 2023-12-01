use pyo3::{PyResult, types::PyModule, Python, pymodule, pyfunction, wrap_pyfunction};
use pyultima::measure::MeasureWrapper;
use frtb_engine::measures::frtb_measure_vec;

#[pyfunction]
fn frtb_measures() -> Vec<MeasureWrapper> {
    let frtb_measures = frtb_measure_vec();

    frtb_measures.into_iter()
        .map(|m|{MeasureWrapper{_inner: m}})
        .collect::<Vec<MeasureWrapper>>()

}

#[pymodule]
#[pyo3(name = "frtb")]
fn frtb_ultibi_engine(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(frtb_measures, m)?)?;
    Ok(())
}
