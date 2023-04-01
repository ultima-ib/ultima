use pyo3::types::PyModule;
use ultibi::polars::lazy::dsl::col;
use ultibi::polars::prelude::Series;
use ultibi::polars::prelude::DataType;
use pyo3::{pyclass, pymethods, Python, PyObject};
use ultibi::Measure;
use crate::conversions::wrappers::{Wrap, POLARS};
/*
#[pyclass]
pub struct MeasureWrapper {
    inner: Measure,
}

#[pymethods]
impl MeasureWrapper {
    #[new]
    fn new(py: Python<'_>,
        name: String,
        inputs: Vec<String>,
        lambda: PyObject,
        output_type: Option<Wrap<DataType>>,) -> Self {
            
            let pypolars = POLARS;

            let function = move |s: &mut [Series]| {
                Python::with_gil(|py| {
                    // this is a python Series
                    let out = call_lambda_with_series_slice(py, s, &lambda, &pypolars);
                
                    // we return an error, because that will become a null value polars lazy apply list
                    //if apply_groups && out.is_none(py) {
                    //    return Ok(None);
                    //}
                
                    Ok(Some(out.to_series(py, &pypolars, "")))
                })
            };

    let exprs = inputs.iter().map(|name| col(name)).collect::<Vec<_>>();

    let output_map = GetOutput::map_field(move |fld| match output_type {
        Some(ref dt) => Field::new(fld.name(), dt.0.clone()),
        None => fld.clone(),
    });
    if apply_groups {
        polars::lazy::dsl::apply_multiple(function, exprs, output_map, returns_scalar).into()
    }
}

pub(crate) fn call_lambda_with_series_slice(
    py: Python,
    s: &mut [Series],
    lambda: &PyObject,
    polars_module: &PyObject,
) -> PyObject {
    let pypolars = polars_module.downcast::<PyModule>(py).unwrap();

    // create a PySeries struct/object for Python
    let iter = s.iter().map(|s| {
        let ps = PySeries::new(s.clone());

        // Wrap this PySeries object in the python side Series wrapper
        let python_series_wrapper = pypolars.getattr("wrap_s").unwrap().call1((ps,)).unwrap();

        python_series_wrapper
    });
    let wrapped_s = PyList::new(py, iter);

    // call the lambda and get a python side Series wrapper
    match lambda.call1(py, (wrapped_s,)) {
        Ok(pyobj) => pyobj,
        Err(e) => panic!("python apply failed: {}", e.value(py)),
    }
}
*/