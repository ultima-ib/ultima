#![allow(clippy::unnecessary_lazy_evaluations)]
extern crate ultibi as ultibi_rs;

use errors::{
    ArrowErrorException, ComputeError, DuplicateError, InvalidOperationError, NoDataError,
    NotFoundError, OtherError, SchemaError, SerdeJsonError, ShapeError, UltimaError,
};
use pyo3::{pyfunction, pymodule, types::PyModule, wrap_pyfunction, PyResult, Python};

mod calculator;
mod conversions;
mod dataset;
mod datasource;
mod errors;
mod filter;
mod measure;
mod requests;

#[pyfunction]
fn agg_ops() -> Vec<&'static str> {
    ultibi_rs::aggregations::BASE_CALCS
        .keys()
        .filter(|el| **el != "scalar")
        .copied()
        .collect::<Vec<&str>>()
}

/// A Python module implemented in Rust.
#[pymodule]
fn ultibi_engine(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(agg_ops, m)?)?;
    //m.add_function(wrap_pyfunction!(exec_agg, m)?)?;
    m.add_class::<requests::AggregationRequestWrapper>()?;
    m.add_class::<requests::ComputeRequestWrapper>()?;
    m.add_class::<dataset::DataSetWrapper>()?;
    m.add_class::<datasource::DataSourceWrapper>()?;
    m.add_class::<measure::MeasureWrapper>()?;
    m.add_class::<filter::FilterWrapper>()?;
    m.add_class::<calculator::CalculatorWrapper>()?;
    m.add_class::<measure::CalcParamWrapper>()?;

    m.add("UltimaError", _py.get_type::<UltimaError>()).unwrap();

    m.add("NotFoundError", _py.get_type::<NotFoundError>())
        .unwrap();
    m.add("ComputeError", _py.get_type::<ComputeError>())
        .unwrap();
    m.add("OtherError", _py.get_type::<OtherError>()).unwrap();
    m.add("NoDataError", _py.get_type::<NoDataError>()).unwrap();
    m.add("ArrowErrorException", _py.get_type::<ArrowErrorException>())
        .unwrap();
    m.add("ShapeError", _py.get_type::<ShapeError>()).unwrap();
    m.add("SchemaError", _py.get_type::<SchemaError>()).unwrap();
    m.add("DuplicateError", _py.get_type::<DuplicateError>())
        .unwrap();
    m.add(
        "InvalidOperationError",
        _py.get_type::<InvalidOperationError>(),
    )
    .unwrap();
    m.add("SerdeJsonError", _py.get_type::<SerdeJsonError>())
        .unwrap();

    Ok(())
}
