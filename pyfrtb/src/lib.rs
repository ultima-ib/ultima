use base_engine::{
    self, execute_aggregation, read_toml2, AggregationRequest, DataFrame, DataSet,
    DataSourceConfig, PolarsResult,
};
use conversion::rust_series_to_py_series;
use frtb_engine::FRTBDataSet;
use pyo3::{exceptions::*, prelude::*, types::PyType};
use std::path::Path;

mod conversion;

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

/// Function to init dataset from config file
//#[pyfunction]
//fn init_frtb_data_set(conf_path: String) -> PyResult<FRTBDataSetWrapper> {
//    if !Path::new(&conf_path).exists() {
//        return Err(PyFileNotFoundError::new_err("file didn't exist"));
//    }
//
//    let Ok(conf) = read_toml2::<DataSourceConfig>(&conf_path) else {
//        return Err(pyo3::exceptions::PyException::new_err("Can not proceed without valid Data Set Up"));
//    };
//
//    let dataset: frtb_engine::FRTBDataSet = DataSet::from_config(conf);
//    Ok(FRTBDataSetWrapper { dataset })
//}
//
#[pyclass]
pub struct AggregationRequestWrapper {
    #[allow(dead_code)]
    ar: AggregationRequest,
}
#[pymethods]
impl AggregationRequestWrapper {
    #[classmethod]
    pub fn from_str(_: &PyType, _str: &str) -> PyResult<Self> {
        let Ok(ar) =
        serde_json::from_str::<AggregationRequest>(&_str) else{
          return Err(pyo3::exceptions::PyException::new_err("Could not parse request"));
        };
        Ok(Self{ar})
    }
    pub fn print(&self) {
        dbg!(&self.ar);
    }
}

/// Function to execute request on prepared data
/// TODO this to take AggregationRequestWrapper and not String
#[pyfunction]
fn _execute_agg(
    request: String,
    prepared_dataset: &FRTBDataSetWrapper,
) 
->PyResult<Vec<PyObject>>
 {
    let Ok(data_req) =
        serde_json::from_str::<AggregationRequest>(&request) else{
          return Err(pyo3::exceptions::PyException::new_err("Could not parse request"));
        };
    
    let Ok(dataframe) = execute_aggregation(data_req, &prepared_dataset.dataset, false) else {
        return Err(pyo3::exceptions::PyException::new_err("Execute aggregation error"));
    };

    dataframe.iter()
        .map(rust_series_to_py_series)
        .collect()
}



#[pyfunction]
fn req_from_str(path: String) -> PyResult<AggregationRequestWrapper> {
    let Ok(ar) =
        serde_json::from_str::<AggregationRequest>(&path) else{
          return Err(pyo3::exceptions::PyException::new_err("Could not parse request"));
        };
        Ok(AggregationRequestWrapper{ar})
}

/// A Python module implemented in Rust.
#[pymodule]
fn frtb_pyengine(_py: Python, m: &PyModule) -> PyResult<()> {
    //m.add_function(wrap_pyfunction!(init_frtb_data_set, m)?)?;
    m.add_function(wrap_pyfunction!(_execute_agg, m)?)?;
    m.add_function(wrap_pyfunction!(req_from_str, m)?)?;
    m.add_class::<AggregationRequestWrapper>()?;
    m.add_class::<FRTBDataSetWrapper>()?;
    Ok(())
}
