use base_engine::{
    self, execute_aggregation, read_toml2, AggregationRequest, DataSet, DataSourceConfig, Series, DataFrame, derive_basic_measures_vec, derive_measure_map, IntoLazy, numeric_columns, Arc
};
use conversion::{rust_series_to_py_series, py_series_to_rust_series};
use errors::{PyUltimaErr, OtherError};
use frtb_engine::FRTBDataSet;
use pyo3::{exceptions::*, prelude::*, types::PyType};
use std::{path::Path, collections::HashMap};

mod conversion;
mod errors;

#[pyclass(subclass)]
struct DataSetWrapper {
    #[allow(dead_code)]
    dataset: Box<dyn DataSet + Send>,
}

//#[pyclass(extends=DataSetWrapper)]
#[pyclass]
struct FRTBDataSetWrapper {
    #[allow(dead_code)]
    dataset: FRTBDataSet,
    //dataset: FRTBDataSet,
}
#[pymethods]
impl FRTBDataSetWrapper {

    #[classmethod]
    fn from_config_path(_: &PyType, conf_path: String) -> PyResult<FRTBDataSetWrapper> {

        if !Path::new(&conf_path).exists() {
            return Err(PyFileNotFoundError::new_err("file doesn't exist"));
        }
    
        let Ok(conf) = read_toml2::<DataSourceConfig>(&conf_path) else {
            return Err(pyo3::exceptions::PyException::new_err("Can not proceed without valid Data Set Up"));
        };
    
        let dataset: frtb_engine::FRTBDataSet = DataSet::from_config(conf);

        Ok(FRTBDataSetWrapper { dataset })
    }

    #[classmethod]
    /// Creates new [FRTBDataSetWrapper]
    fn new(_: &PyType, _py: Python, seriess: Vec<Py<PyAny>>, measures: Option<Vec<String>>, build_params: Option<HashMap<String, String>>) -> PyResult<FRTBDataSetWrapper> {

        let df = DataFrame::new (
            seriess.into_iter()
            .map(|x|{
                py_series_to_rust_series( x.as_ref(_py) )
            })
            .collect::<PyResult<Vec<Series>>>()?
        ).map_err(|err|PyUltimaErr::Polars(err))?;
        let schema = df.schema();
        let arc_schema = Arc::new(schema);
        // If measures is None - assume all numeric column
        let measures = measures.unwrap_or_else(||numeric_columns(arc_schema));
        let mv = derive_basic_measures_vec(measures);
        let mm = derive_measure_map(mv);
        
        let build_params = build_params.unwrap_or_default();
    
        let dataset: frtb_engine::FRTBDataSet = DataSet::new(df.lazy(), mm, build_params);

        Ok(FRTBDataSetWrapper { dataset })
    }

    pub fn prepare(&mut self) -> PyResult<()> {
        let lf = self.dataset.get_lazyframe().clone();

        let new_frame = self.dataset.prepare_frame(Some(lf))
            .map_err(|err| PyUltimaErr::Polars(err))?;

        self.dataset.set_lazyframe_inplace(new_frame);
        Ok(())
    }

    pub fn measures(&self) -> HashMap<String, Option<&str>> {

        self.dataset
            .get_measures()
            .iter()
            .map(|(x, m)| (x.to_string(), m.aggregation))
            .collect::<HashMap<String, Option<&str>>>()

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
fn exec_agg(
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
    m.add_function(wrap_pyfunction!(exec_agg, m)?)?;
    m.add_class::<AggregationRequestWrapper>()?;
    m.add_class::<FRTBDataSetWrapper>()?;
    m.add(
        "OtherError",
        _py.get_type::<OtherError>(),
    )
    .unwrap();

    Ok(())
}
