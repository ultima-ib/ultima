use base_engine::{
    self, execute_aggregation, read_toml2, AggregationRequest, DataFrame, DataSet,
    DataSourceConfig, PolarsResult,
};
use conversion::rust_series_to_py_series;
use frtb_engine::FRTBDataSet;
use pyo3::{exceptions::*, prelude::*, types::PyType};
use std::path::Path;

mod conversion;
mod errors;

#[pyclass]
struct FRTBDataSetWrapper {
    #[allow(dead_code)]
    dataset: FRTBDataSet,
}
#[pymethods]
impl FRTBDataSetWrapper {

    pub fn prepare(&mut self) -> PyResult<()> {
        let lf = self.dataset.get_lazyframe().clone();
        let new_frame = self.dataset.prepare_frame(Some(lf))
            .map_err(|_|PyErr::new::<PyTypeError, _>("Failed to prepare dataset"))?;
        
        self.dataset.set_lazyframe_inplace(new_frame);
        Ok(())
    }

    #[classmethod]
    fn from_config_path(_: &PyType, conf_path: String) -> PyResult<FRTBDataSetWrapper> {
        if !Path::new(&conf_path).exists() {
            return Err(PyFileNotFoundError::new_err("file didn't exist"));
        }
    
        let Ok(conf) = read_toml2::<DataSourceConfig>(&conf_path) else {
            return Err(pyo3::exceptions::PyException::new_err("Can not proceed without valid Data Set Up"));
        };
    
        let dataset: frtb_engine::FRTBDataSet = DataSet::from_config(conf);
        Ok(FRTBDataSetWrapper { dataset })
    }
}

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
            Ok(ar) => Ok(Self{ar}),
            Err(err) => Err(errors::PyUltimaErr::from(err).into()),
        }
        
    }
    
    /// Format `AggregationRequest` as String
    pub fn as_str(&self) -> String {
        format!("{:?}", self.ar)
    }
}

/// Function to execute request on prepared data
#[pyfunction]
fn _execute_agg(
    request: AggregationRequestWrapper,
    prepared_dataset: &FRTBDataSetWrapper,
    streaming: bool
) 
->PyResult<Vec<PyObject>>
 {
    
    let dataframe = execute_aggregation(request.ar, &prepared_dataset.dataset, streaming)
        .map_err(|err| errors::PyUltimaErr::Polars(err))?;

    dataframe.iter()
        .map(rust_series_to_py_series)
        .collect()
}

/// A Python module implemented in Rust.
#[pymodule]
fn frtb_pyengine(_py: Python, m: &PyModule) -> PyResult<()> {
    //m.add_function(wrap_pyfunction!(init_frtb_data_set, m)?)?;
    m.add_function(wrap_pyfunction!(_execute_agg, m)?)?;
    m.add_class::<AggregationRequestWrapper>()?;
    m.add_class::<FRTBDataSetWrapper>()?;
    Ok(())
}
