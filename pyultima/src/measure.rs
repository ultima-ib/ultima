use std::sync::Arc;

use polars::lazy::dsl::GetOutput;
use polars::lazy::dsl::apply_multiple;
use pyo3::types::IntoPyDict;
use pyo3::types::PyList;
//use pyo3::types::PyModule;
use ultibi::BaseMeasure;
use ultibi::CPM;
//use ultibi::Calculator;
use ultibi::polars::lazy::dsl::col;
use ultibi::polars::prelude::Series;
use ultibi::polars::prelude::DataType;
use pyo3::{pyclass, pymethods, Python, PyObject};
use ultibi::Measure;
use crate::conversions::series::py_series_to_rust_series;
use crate::conversions::series::rust_series_to_py_series;
use crate::conversions::wrappers::{Wrap};

#[pyclass]
#[derive(Clone)]
pub struct MeasureWrapper {
    pub _inner: Measure,
}

#[pymethods]
impl MeasureWrapper {
    #[new]
    fn new(_py: Python<'_>,
        name: String,
        inputs: Vec<String>,
        lambda: PyObject,
        output_type: Wrap<DataType>,
        returns_scalar: bool) -> Self {

        let exprs = inputs.iter().map(|name| col(name)).collect::<Vec<_>>();

        let output = GetOutput::from_type(output_type.0);

        // This to go inside apply_multiple  
        //let function = move |s: &mut [Series]| {
        //    let l = lambda.clone();
        //    //let pp = POLARS.clone();
        //    Python::with_gil(move |py| {
        //        // this is a python Series
        //        
        //        let out = call_lambda_with_series_slice(py, s, &l);
        //    
        //        // we return an error, because that will become a null value polars lazy apply list
        //        //if apply_groups && out.is_none(py) {
        //        //    return Ok(None);
        //        //}
        //        let srs = py_series_to_rust_series(out.as_ref(py)).ok(); // convert Res to Option
        //    
        //        Ok(srs)
        //    })
        //};

    // Convert function into Expr
    let calculator = move |op: &CPM| {
        let l = lambda.clone();
        let params = op.clone();

        Ok(
            apply_multiple(
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
         returns_scalar))
    };

    let boxed_calc = Arc::new(calculator);

    let inner: Measure = BaseMeasure{name, calculator: boxed_calc, ..Default::default()}.into();

    Self{_inner:inner}
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
    let iter = s.iter().map(|s| {
        rust_series_to_py_series(s).unwrap()
    });
    let wrapped_s = PyList::new(py, iter);

    // call the lambda and get a python side Series wrapper
    match lambda.call(py, (wrapped_s,), Some(kwargs.into_py_dict(py))) {
        Ok(pyobj) => pyobj,
        Err(e) => panic!("python apply failed: {}", e.value(py)),
    }
}
