use polars::series::Series;
use pyo3::{pyclass, pymethods, types::PyType, Python, PyAny, Py, PyResult};
use ultibi::{datasource::DataSource, DataFrame};

use crate::{conversions::series::py_series_to_rust_series, errors::PyUltimaErr};


#[pyclass]
#[derive(Clone)]
pub struct DataSourceWrapper {
    #[allow(dead_code)]
    pub inner: DataSource,
}
#[pymethods]
impl DataSourceWrapper { 

    /// In Memory
    #[classmethod]
    fn from_frame(_: &PyType,
        py: Python,
        seriess: Vec<Py<PyAny>>) -> PyResult<Self> {
            let df = DataFrame::new(
                seriess
                    .into_iter()
                    .map(|x| py_series_to_rust_series(x.as_ref(py)))
                    .collect::<PyResult<Vec<Series>>>()?,
            )
            .map_err(PyUltimaErr::Polars)?;

        Ok(DataSourceWrapper{inner: DataSource::InMemory(df)})
    }
}