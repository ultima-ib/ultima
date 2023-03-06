use pyo3::{pyclass, pymethods, types::PyType, PyResult};
use ultibi::{AggregationRequest, ComputeRequest};

use crate::errors;

#[pyclass]
#[derive(Clone)]
pub struct AggregationRequestWrapper {
    #[allow(dead_code)]
    pub ar: AggregationRequest,
}
#[pymethods]
impl AggregationRequestWrapper {
    #[classmethod]
    /// Converts str into AggregationRequest
    pub fn from_str(_: &PyType, json_str: &str) -> PyResult<Self> {
        match serde_json::from_str::<AggregationRequest>(json_str) {
            Ok(ar) => Ok(Self { ar }),
            Err(err) => Err(errors::PyUltimaErr::from(err).into()),
        }
    }

    /// Format `AggregationRequest` as String
    pub fn as_str(&self) -> String {
        format!("{:?}", self.ar)
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ComputeRequestWrapper {
    #[allow(dead_code)]
    pub ar: ComputeRequest,
}
#[pymethods]
impl ComputeRequestWrapper {
    #[classmethod]
    /// Converts str into AggregationRequest
    pub fn from_str(_: &PyType, json_str: &str) -> PyResult<Self> {
        match serde_json::from_str::<ComputeRequest>(json_str) {
            Ok(ar) => Ok(Self { ar }),
            Err(err) => Err(errors::PyUltimaErr::from(err).into()),
        }
    }

    /// Format `AggregationRequest` as String
    pub fn as_str(&self) -> String {
        format!("{:?}", self.ar)
    }
}
