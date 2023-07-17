use std::ops::Deref;

use polars::prelude::LogicalPlan;
use pyo3::{pyclass, pymethods, types::PyType, FromPyObject, Py, PyAny, PyResult, Python};
use ultibi::polars::{prelude::LazyFrame, series::Series};
use ultibi::{datasource::DataSource, DataFrame};

use crate::{conversions::series::py_series_to_rust_series, errors::PyUltimaErr};

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
}

pub struct PyLazyFrame(pub LazyFrame);

impl<'a> FromPyObject<'a> for PyLazyFrame {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s = ob.call_method0("__getstate__")?.extract::<Vec<u8>>()?;
        let lp: LogicalPlan = ciborium::de::from_reader(&*s).map_err(
            |e| PyUltimaErr::Other(
                format!("Error when deserializing LazyFrame. This may be due to mismatched polars versions. {}", e)
            )
        )?;
        Ok(PyLazyFrame(LazyFrame::from(lp)))
    }
}
