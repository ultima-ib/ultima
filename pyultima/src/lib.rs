use base_engine::polars::prelude::Series;
use base_engine::{
    self, derive_basic_measures_vec, execute_aggregation, numeric_columns, read_toml2,
    AggregationRequest, DataFrame, DataSet, DataSetBase, DataSourceConfig, IntoLazy, MeasuresMap,
    ValidateSet,
};
use conversion::{py_series_to_rust_series, rust_series_to_py_series};
use errors::{
    ArrowErrorException, ComputeError, DuplicateError, InvalidOperationError, NoDataError,
    NotFoundError, OtherError, PyUltimaErr, SchemaError, SerdeJsonError, ShapeError,
};
use frtb_engine::FRTBDataSet;
use pyo3::{exceptions::*, prelude::*, types::PyType, PyTypeInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::{collections::BTreeMap, path::Path};

mod conversion;
mod errors;

#[pyclass]
struct DataSetWrapper {
    dataset: Box<dyn DataSet>,
}

fn from_conf<T: DataSet + 'static>(conf_path: String) -> PyResult<DataSetWrapper> {
    if !Path::new(&conf_path).exists() {
        return Err(PyFileNotFoundError::new_err("file doesn't exist"));
    }

    let Ok(conf) = read_toml2::<DataSourceConfig>(&conf_path) else {
        return Err(pyo3::exceptions::PyException::new_err("Can not proceed without valid Data Set Up"));
    };

    let dataset: Box<T> = Box::new(DataSet::from_config(conf));
    Ok(DataSetWrapper { dataset })
}

fn from_frame<T: DataSet + 'static>(
    py: Python,
    seriess: Vec<Py<PyAny>>,
    measures: Option<Vec<String>>,
    build_params: Option<BTreeMap<String, String>>,
) -> PyResult<DataSetWrapper> {
    let df = DataFrame::new(
        seriess
            .into_iter()
            .map(|x| py_series_to_rust_series(x.as_ref(py)))
            .collect::<PyResult<Vec<Series>>>()?,
    )
    .map_err(PyUltimaErr::Polars)?;

    let schema = df.schema();
    let arc_schema = Arc::new(schema);
    // If measures is None - assume all numeric column
    let measures = measures.unwrap_or_else(|| numeric_columns(arc_schema));
    let mv = derive_basic_measures_vec(measures);
    let mm: MeasuresMap = MeasuresMap::from_iter(mv);

    let build_params = build_params.unwrap_or_default();

    let dataset: T = DataSet::new(df.lazy(), mm, build_params);
    let dataset = Box::new(dataset);

    Ok(DataSetWrapper { dataset })
}

#[pymethods]
impl DataSetWrapper {
    #[new]
    fn new(py: Python<'_>) -> Self {
        // get a &PyType corresponding to Self
        let pyself = Self::type_object(py);
        Self::from_frame(pyself, py, vec![], None, None).unwrap()
    }

    #[classmethod]
    fn from_config_path(_: &PyType, conf_path: String) -> PyResult<Self> {
        from_conf::<DataSetBase>(conf_path)
    }

    #[classmethod]
    fn frtb_from_config_path(_: &PyType, conf_path: String) -> PyResult<Self> {
        from_conf::<FRTBDataSet>(conf_path)
    }

    #[classmethod]
    fn from_frame(
        _: &PyType,
        py: Python,
        seriess: Vec<Py<PyAny>>,
        measures: Option<Vec<String>>,
        build_params: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        from_frame::<DataSetBase>(py, seriess, measures, build_params)
    }

    #[classmethod]
    fn frtb_from_frame(
        _: &PyType,
        py: Python,
        seriess: Vec<Py<PyAny>>,
        measures: Option<Vec<String>>,
        build_params: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        from_frame::<FRTBDataSet>(py, seriess, measures, build_params)
    }

    pub fn prepare(&mut self) -> PyResult<()> {
        let lf = self.dataset.get_lazyframe().clone();

        let new_frame = self
            .dataset
            .prepare_frame(Some(lf))
            .map_err(PyUltimaErr::Polars)?;

        self.dataset.set_lazyframe_inplace(new_frame);
        Ok(())
    }

    pub fn measures(&self) -> BTreeMap<String, Option<&str>> {
        self.dataset
            .get_measures()
            .iter()
            .map(|(x, m)| (x.to_string(), *m.aggregation()))
            .collect::<BTreeMap<String, Option<&str>>>()
    }
    pub fn frame(&self) -> PyResult<Vec<PyObject>> {
        self.dataset
            .get_lazyframe()
            .clone()
            .collect()
            .map_err(PyUltimaErr::Polars)?
            .iter()
            .map(rust_series_to_py_series)
            .collect()
    }
    pub fn fields(&self) -> PyResult<Vec<String>> {
        let schema = self
            .dataset
            .get_lazyframe()
            .schema()
            .map_err(PyUltimaErr::Polars)?;

        Ok(base_engine::prelude::fields_columns(schema))
    }
    pub fn calc_params(&self) -> PyResult<Vec<HashMap<&str, Option<String>>>> {
        let name = "name";
        let hint = "hint";

        let res = self
            .dataset
            .calc_params()
            .iter()
            .map(|calc_param| {
                HashMap::from([
                    (name, Some(calc_param.name.to_string())),
                    (hint, calc_param.type_hint.map(str::to_string)),
                ])
            })
            .collect::<Vec<HashMap<&str, Option<String>>>>();

        Ok(res)
    }

    pub fn validate(&self) -> PyResult<()> {
        self.dataset
            .validate_frame(None, ValidateSet::ALL)
            .map_err(errors::PyUltimaErr::Polars)?;

        Ok(())
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
            Ok(ar) => Ok(Self { ar }),
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
    prepared_dataset: &DataSetWrapper,
    streaming: bool,
) -> PyResult<Vec<PyObject>> {
    let dataframe = execute_aggregation(&request.ar, prepared_dataset.dataset.as_ref(), streaming)
        .map_err(errors::PyUltimaErr::Polars)?;

    dataframe.iter().map(rust_series_to_py_series).collect()
}

#[pyfunction]
fn agg_ops() -> Vec<&'static str> {
    base_engine::api::aggregations::BASE_CALCS
        .keys()
        .filter(|el| **el != "scalar")
        .copied()
        .collect::<Vec<&str>>()
}

/// A Python module implemented in Rust.
#[pymodule]
fn ultima_pyengine(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(agg_ops, m)?)?;
    m.add_function(wrap_pyfunction!(exec_agg, m)?)?;
    m.add_class::<AggregationRequestWrapper>()?;
    m.add_class::<DataSetWrapper>()?;

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
