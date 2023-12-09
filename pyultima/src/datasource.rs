use std::ops::Deref;

use pyo3::{pyclass, pymethods, types::PyType, Py, PyAny, PyResult, Python};
use ultibi::polars::series::Series;
use ultibi::{datasource::DataSource, DataFrame};

use crate::{
    conversions::{lazy::PyLazyFrame, series::py_series_to_rust_series},
    db::DbInfo,
    errors::PyUltimaErr,
};

#[pyclass]
#[derive(Clone)]
pub struct DataSourceWrapper {
    #[allow(dead_code)]
    pub inner: DataSource,
}

impl Deref for DataSourceWrapper {
    type Target = DataSource;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[pymethods]
impl DataSourceWrapper {
    /// In Memory
    #[classmethod]
    fn from_frame(_: &PyType, py: Python, seriess: Vec<Py<PyAny>>) -> PyResult<Self> {
        let df = DataFrame::new(
            seriess
                .into_iter()
                .map(|x| py_series_to_rust_series(x.as_ref(py)))
                .collect::<PyResult<Vec<Series>>>()?,
        )
        .map_err(PyUltimaErr::Polars)?;

        Ok(DataSourceWrapper { inner: df.into() })
    }

    /// Should be used for a scan only
    #[classmethod]
    fn from_scan(_: &PyType, _py: Python, pylf: PyLazyFrame) -> PyResult<Self> {
        let lf = pylf.0;
        Ok(DataSourceWrapper { inner: lf.into() })
    }

    #[classmethod]
    fn from_db(_: &PyType, _py: Python, db: DbInfo) -> PyResult<Self> {
        let db = db.inner;
        Ok(DataSourceWrapper { inner: db.into() })
    }
}
