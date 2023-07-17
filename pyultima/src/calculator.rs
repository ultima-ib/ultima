use std::sync::Arc;

use crate::conversions::{
    series::{py_series_to_rust_series, rust_series_to_py_series},
    wrappers::Wrap,
};
use polars::prelude::PolarsError;
use pyo3::types::PyBytes;
use pyo3::{
    pyclass, pymethods,
    types::{IntoPyDict, PyList, PyType},
    PyObject, PyResult, Python,
};
use ultibi::polars::lazy::dsl::GetOutput;
use ultibi::polars::prelude::Expr;
use ultibi::polars::prelude::Series;
use ultibi::polars::{
    lazy::dsl::{apply_multiple, col},
    prelude::DataType,
};
use ultibi::{Calculator, PolarsResult, CPM};

#[pyclass]
#[derive(Clone)]
pub struct CalculatorWrapper {
    #[allow(dead_code)]
    pub inner: Calculator,
}
#[pymethods]
impl CalculatorWrapper {
    #[classmethod]
    pub fn standard(_: &PyType, lambda: PyObject) -> PyResult<Self> {
        let calculator = move |op: &CPM| {
            let lambda = lambda.clone();

            Python::with_gil(move |py| {
                // this is a python Series
                let out = lambda
                    .call(py, (op.into_py_dict(py),), None)
                    .map_err(|e| PolarsError::ComputeError(e.value(py).to_string().into()))?;
                let b = out.call_method(py, "__getstate__", (), None).unwrap(); // should never fail
                let as_pybytes = b.downcast::<PyBytes>(py).unwrap(); // should never fail
                let rustbytes = as_pybytes.as_bytes();
                //let e = serde_json::from_slice::<Expr>(rustbytes).unwrap(); // should never fail
                let e: Expr = ciborium::de::from_reader(rustbytes).map_err(|e| {
                    PolarsError::ComputeError(format!("Error deserializing expression. This could be due to differenet Polars version. Try using Custom calculator. {}", e).into())
                })?;
                Ok(e)
            })
        };
        let calculator = Arc::new(calculator);
        Ok(Self { inner: calculator })
    }

    #[classmethod]
    /// Format `AggregationRequest` as String
    pub fn custom(
        _: &PyType,
        lambda: PyObject,
        output_type: Wrap<DataType>,
        inputs: Vec<String>,
        returns_scalar: bool,
    ) -> PyResult<Self> {
        let exprs = inputs.iter().map(|name| col(name)).collect::<Vec<_>>();

        let output = GetOutput::from_type(output_type.0);

        // Convert function into Expr
        let calculator = move |op: &CPM| {
            let l = lambda.clone();
            let params = op.clone();

            Ok(apply_multiple(
                move |s: &mut [Series]| {
                    let ll = l.clone();
                    let args = params.clone();

                    Python::with_gil(move |py| {
                        // this is a python Series
                        let out = call_lambda_with_args_and_series_slice(py, &args, s, &ll)?;

                        // we return an error, because that will become a null value polars lazy apply list
                        if out.is_none(py) {
                            return Ok(None);
                        }
                        let srs = py_series_to_rust_series(out.as_ref(py)).ok(); // convert Res to Option

                        Ok(srs)
                    })
                },
                exprs.clone(),
                output.clone(),
                returns_scalar,
            ))
        };

        let calculator = Arc::new(calculator);

        Ok(Self { inner: calculator })
    }
}

pub(crate) fn call_lambda_with_args_and_series_slice(
    py: Python,
    kwargs: &CPM,
    s: &mut [Series],
    lambda: &PyObject,
) -> PolarsResult<PyObject> {
    // create a PySeries struct/object for Python
    let iter = s.iter().map(|s| rust_series_to_py_series(s).unwrap());
    let wrapped_s = PyList::new(py, iter);
    lambda
        .call(py, (wrapped_s, kwargs.into_py_dict(py)), None)
        .map_err(|e| PolarsError::ComputeError(e.value(py).to_string().into()))
}
