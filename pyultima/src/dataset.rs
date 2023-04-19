use frtb_engine::FRTBDataSet;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::{prelude::*, types::PyType, PyTypeInfo};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
//use std::sync::Mutex;
use crate::conversions::series::{py_series_to_rust_series, rust_series_to_py_series};
use crate::errors::PyUltimaErr;
use crate::measure::MeasureWrapper;
use crate::requests;
use std::sync::RwLock;
use ultibi::polars::prelude::Series;
use ultibi::VisualDataSet;
use ultibi::{
    self, derive_basic_measures_vec, numeric_columns, DataFrame, DataSet, DataSetBase, IntoLazy,
    MeasuresMap, ValidateSet,
};

#[pyclass]
pub struct DataSetWrapper {
    dataset: Arc<RwLock<dyn DataSet>>,
}

/// Part of config is limit of numeric cols
fn from_conf<T: DataSet + 'static>(
    conf_path: String,
    collect: bool,
    prepare: bool,
    bespoke_measures: MeasuresMap,
) -> PyResult<DataSetWrapper> {
    // This is now done in build_validate_prepare
    // TODO build_validate_prepare to return result and errors to be mapped
    // Like this:
    if !Path::new(&conf_path).exists() {
        return Err(PyFileNotFoundError::new_err("Config file doesn't exist"));
    }

    let ds = ultibi::acquire::config_build_validate_prepare::<T>(
        conf_path.as_str(),
        collect,
        prepare,
        bespoke_measures,
    );
    //let dataset = Box::new(ds);
    Ok(DataSetWrapper {
        dataset: Arc::new(RwLock::new(ds)),
    })
}

fn from_frame<T: DataSet + 'static>(
    py: Python,
    seriess: Vec<Py<PyAny>>,
    measures: Option<Vec<String>>,
    build_params: BTreeMap<String, String>,
    bespoke_measures: MeasuresMap,
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
    // TODO function arg should be add_numeric_columns_as_measures, defaulted to true
    let measures = measures.unwrap_or_else(|| numeric_columns(arc_schema));
    let mv = derive_basic_measures_vec(measures);
    let mut mm: MeasuresMap = MeasuresMap::from_iter(mv);
    mm.extend(bespoke_measures);

    let ds: T = DataSet::new(df.lazy(), mm, build_params);

    Ok(DataSetWrapper {
        dataset: Arc::new(RwLock::new(ds)),
    })
}

#[pymethods]
impl DataSetWrapper {
    #[new]
    fn new(py: Python<'_>) -> Self {
        // get a &PyType corresponding to Self
        let pyself = Self::type_object(py);
        Self::from_frame(pyself, py, vec![], None, None, None).unwrap()
    }

    #[classmethod]
    fn from_config_path(
        _: &PyType,
        conf_path: String,
        collect: Option<bool>,
        prepare: Option<bool>,
        bespoke_measures: Option<Vec<MeasureWrapper>>,
    ) -> PyResult<Self> {
        let collect = collect.unwrap_or_else(|| true);
        let prepare = prepare.unwrap_or_else(|| false);
        let bespoke_measures = bespoke_measures.unwrap_or_default();
        let mm = bespoke_measures
            .into_iter()
            .map(|x| {
                let m = x._inner;
                (m.name().clone(), m)
            })
            .collect::<MeasuresMap>();
        from_conf::<DataSetBase>(conf_path, collect, prepare, mm)
    }

    #[classmethod]
    fn frtb_from_config_path(
        _: &PyType,
        conf_path: String,
        collect: Option<bool>,
        prepare: Option<bool>,
    ) -> PyResult<Self> {
        let collect = collect.unwrap_or_else(|| true);
        let prepare = prepare.unwrap_or_else(|| false);
        let empty = BTreeMap::default();
        from_conf::<FRTBDataSet>(conf_path, collect, prepare, empty)
    }

    #[classmethod]
    fn from_frame(
        _: &PyType,
        py: Python,
        seriess: Vec<Py<PyAny>>,
        measures: Option<Vec<String>>,
        build_params: Option<BTreeMap<String, String>>,
        bespoke_measures: Option<Vec<MeasureWrapper>>,
    ) -> PyResult<Self> {
        let build_params = build_params.unwrap_or_default();
        let mm = bespoke_measures
            .unwrap_or_default()
            .into_iter()
            .map(|x| {
                let m = x._inner;
                (m.name().clone(), m)
            })
            .collect::<MeasuresMap>();
        from_frame::<DataSetBase>(py, seriess, measures, build_params, mm)
    }

    #[classmethod]
    fn frtb_from_frame(
        _: &PyType,
        py: Python,
        series: Vec<Py<PyAny>>,
        measures: Option<Vec<String>>,
        build_params: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        let build_params = build_params.unwrap_or_default();
        let empty = BTreeMap::default();
        from_frame::<FRTBDataSet>(py, series, measures, build_params, empty)
    }

    pub fn ui(&self, py: Python, streaming: bool) -> PyResult<()> {
        let a = Arc::clone(&self.dataset);
        py.allow_threads(|| a.ui(streaming));
        Ok(())
    }

    pub fn prepare(&mut self, collect: Option<bool>) -> PyResult<()> {
        let ds = self.dataset.read().expect("Poisonned RwLock");
        let lf = ds.get_lazyframe().clone();
        let collect = collect.unwrap_or_else(|| true);

        let mut new_frame = ds.prepare_frame(Some(lf)).map_err(PyUltimaErr::Polars)?;

        if collect {
            new_frame = new_frame.collect().map_err(PyUltimaErr::Polars)?.lazy()
        }
        std::mem::drop(ds);

        let mut ds = self.dataset.write().expect("Poisonned RwLock");
        ds.set_lazyframe_inplace(new_frame);
        Ok(())
    }

    pub fn compute(
        &self,
        py: Python,
        request: requests::ComputeRequestWrapper,
        streaming: bool,
    ) -> PyResult<Vec<PyObject>> {
        py.allow_threads(|| {
            self.dataset
                .read()
                .expect("Poisonned RwLock")
                .compute(request.ar, streaming)
                .map_err(PyUltimaErr::Polars)?
                .iter()
                .map(rust_series_to_py_series)
                .collect()
        })
    }

    pub fn measures(&self) -> BTreeMap<String, Option<String>> {
        self.dataset
            .read()
            .expect("Poisonned RwLock")
            .get_measures()
            .iter()
            .map(|(x, m)| (x.to_string(), m.aggregation().clone()))
            .collect::<BTreeMap<String, Option<String>>>()
    }
    pub fn frame(&self) -> PyResult<Vec<PyObject>> {
        self.dataset
            .read()
            .expect("Poisonned RwLock")
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
            .read()
            .expect("Poisonned RwLock")
            .get_lazyframe()
            .schema()
            .map_err(PyUltimaErr::Polars)?;

        Ok(ultibi::prelude::fields_columns(schema))
    }
    pub fn calc_params(&self) -> Vec<(String, Option<String>, Option<String>)> {
        self.dataset
            .read()
            .expect("Poisonned RwLock")
            .calc_params()
            .iter()
            .cloned()
            .map(|calc_param| (calc_param.name, calc_param.type_hint, calc_param.default))
            .collect()
    }

    pub fn validate(&self) -> PyResult<()> {
        self.dataset
            .read()
            .expect("Poisonned RwLock")
            .validate_frame(None, ValidateSet::ALL)
            .map_err(PyUltimaErr::Polars)?;

        Ok(())
    }
}
