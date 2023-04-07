//! Note Measure is not ready for release. Blocked due to
//! https://github.com/pola-rs/polars/issues/8039

use std::sync::Arc;

use polars::lazy::dsl::apply_multiple;
use polars::lazy::dsl::GetOutput;
use pyo3::types::IntoPyDict;
use pyo3::types::PyList;
use pyo3::types::PyType;
//use pyo3::types::PyModule;
use ultibi::filters::fltr_chain;
use ultibi::filters::FilterE;
use ultibi::BaseMeasure;
use ultibi::DependantMeasure;
use ultibi::CPM;
//use ultibi::Calculator;
use crate::conversions::series::py_series_to_rust_series;
use crate::conversions::series::rust_series_to_py_series;
use crate::conversions::wrappers::Wrap;
use crate::filter::FilterWrapper;
use pyo3::{pyclass, pymethods, PyObject, Python};
use ultibi::polars::lazy::dsl::col;
use ultibi::polars::prelude::DataType;
use ultibi::polars::prelude::Series;
use ultibi::Measure;

#[pyclass]
#[derive(Clone)]
pub struct MeasureWrapper {
    pub _inner: Measure,
}

#[pymethods]
impl MeasureWrapper {
    #[classmethod]
    fn new_basic(
        _: &PyType,
        name: String,
        lambda: PyObject,
        output_type: Wrap<DataType>,
        inputs: Vec<String>,
        returns_scalar: bool,
        precompute_filter: Vec<Vec<FilterWrapper>>,
        aggregation_restriction: Option<String>,
    ) -> Self {
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
                        let out = call_lambda_with_args_and_series_slice(py, &args, s, &ll);

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

        let precompute_filters = precompute_filter
            .into_iter()
            .map(|or| {
                or.into_iter()
                    .map(|fltr| fltr.inner)
                    .collect::<Vec<FilterE>>()
            })
            .collect::<Vec<Vec<FilterE>>>();

        let precomputefilter = fltr_chain(&precompute_filters);

        let inner: Measure = BaseMeasure {
            name,
            calculator,
            precomputefilter,
            aggregation: aggregation_restriction,
        }
        .into();

        Self { _inner: inner }
    }

    #[classmethod]
    fn new_dependant(
        _: &PyType,
        name: String,
        lambda: PyObject,
        output_type: Wrap<DataType>,
        inputs: Vec<String>,
        returns_scalar: bool,
        depends_upon: Vec<(String, String)>,
    ) -> Self {
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
                        let out = call_lambda_with_args_and_series_slice(py, &args, s, &ll);

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

        let boxed_calc = Arc::new(calculator);

        let inner: Measure = DependantMeasure {
            name,
            calculator: boxed_calc,
            depends_upon,
        }
        .into();

        Self { _inner: inner }
    }
}

pub(crate) fn call_lambda_with_args_and_series_slice(
    py: Python,
    kwargs: &CPM,
    s: &mut [Series],
    lambda: &PyObject,
    //polars_module: &PyObject,
) -> PyObject {
    //let pypolars = polars_module.downcast::<PyModule>(py).unwrap();

    // create a PySeries struct/object for Python
    let iter = s.iter().map(|s| rust_series_to_py_series(s).unwrap());
    let wrapped_s = PyList::new(py, iter);
    dbg!("HERE");

    // call the lambda and get a python side Series wrapper
    match lambda.call(py, (wrapped_s, kwargs.into_py_dict(py)), None) {
        Ok(pyobj) => pyobj,
        Err(e) => panic!("python apply failed: {}", e.value(py)),
    }
}
