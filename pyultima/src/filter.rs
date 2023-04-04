use pyo3::{pyclass, pymethods, types::PyType, PyResult};
use ultibi::filters::FilterE;

use crate::errors;

#[pyclass]
#[derive(Clone)]
pub struct FilterWrapper {
    #[allow(dead_code)]
    pub inner: FilterE,
}
#[pymethods]
impl FilterWrapper {
    #[classmethod]
    /// Converts str into AggregationRequest
    pub fn from_str(_: &PyType, json_str: &str) -> PyResult<Self> {
        match serde_json::from_str::<FilterE>(json_str) {
            Ok(f) => Ok(Self { inner: f }),
            Err(err) => Err(errors::PyUltimaErr::from(err).into()),
        }
    }

    /// Format `AggregationRequest` as String
    pub fn as_str(&self) -> String {
        format!("{:?}", self.inner)
    }
}
