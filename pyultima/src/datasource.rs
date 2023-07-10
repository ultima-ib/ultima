use pyo3::{pyclass, pymethods};
use ultibi::{datasource::DataSource};


#[pyclass]
#[derive(Clone)]
pub struct DataSourceWrapper {
    #[allow(dead_code)]
    pub inner: DataSource,
}
#[pymethods]
impl DataSourceWrapper { }