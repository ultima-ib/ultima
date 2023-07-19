use frtb_engine::FRTBDataSet;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::{prelude::*, types::PyType, PyTypeInfo};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use ultibi::datasource::DataSource;
use ultibi::filters::FilterE;
use ultibi::new::NewSourcedDataSet;
//use std::sync::Mutex;
use crate::conversions::series::{py_series_to_rust_series, rust_series_to_py_series};
use crate::datasource::DataSourceWrapper;
use crate::errors::PyUltimaErr;
use crate::filter::FilterWrapper;
use crate::measure::MeasureWrapper;
use crate::requests;
use std::sync::RwLock;
use ultibi::polars::prelude::Series;
use ultibi::VisualDataSet;
use ultibi::{
    self, derive_basic_measures_vec, numeric_columns, DataFrame, DataSet, DataSetBase, MeasuresMap,
};

#[pyclass]
pub struct DataSetWrapper {
    dataset: Arc<RwLock<dyn DataSet>>,
}

/// Part of config is limit of numeric cols
fn from_conf<T: NewSourcedDataSet + 'static>(
    conf_path: String,
    bespoke_measures: MeasuresMap,
) -> PyResult<DataSetWrapper> {
    // This is now done in build_validate_prepare
    // TODO build_validate_prepare to return result and errors to be mapped
    // Like this:
    if !Path::new(&conf_path).exists() {
        return Err(PyFileNotFoundError::new_err("Config file doesn't exist"));
    }

    let ds =
        ultibi::acquire::config_build_validate_prepare::<T>(conf_path.as_str(), bespoke_measures);
    //let dataset = Box::new(ds);
    Ok(DataSetWrapper {
        dataset: Arc::new(RwLock::new(ds)),
    })
}

fn from_source<T: NewSourcedDataSet + 'static>(
    _: Python,
    source: DataSource,
    measures: Option<Vec<String>>,
    build_params: BTreeMap<String, String>,
    bespoke_measures: MeasuresMap,
) -> PyResult<DataSetWrapper> {
    let arc_schema = source.get_schema().map_err(PyUltimaErr::Ultima)?;
    let measures = measures.unwrap_or_else(|| numeric_columns(arc_schema));
    let mv = derive_basic_measures_vec(measures);
    let mut mm: MeasuresMap = MeasuresMap::from_iter(mv);
    mm.extend(bespoke_measures);

    let ds: T = T::new(source, mm, Default::default(), build_params);

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
        bespoke_measures: Option<Vec<MeasureWrapper>>,
    ) -> PyResult<Self> {
        let bespoke_measures = bespoke_measures.unwrap_or_default();
        let mm = bespoke_measures
            .into_iter()
            .map(|x| {
                let m = x._inner;
                (m.name().clone(), m)
            })
            .collect::<MeasuresMap>();
        from_conf::<DataSetBase>(conf_path, mm)
    }

    #[classmethod]
    fn frtb_from_config_path(
        _: &PyType,
        conf_path: String,
        bespoke_measures: Option<Vec<MeasureWrapper>>,
    ) -> PyResult<Self> {
        let bespoke_measures = bespoke_measures.unwrap_or_default();
        let mm = bespoke_measures
            .into_iter()
            .map(|x| x._inner)
            .collect::<MeasuresMap>();

        from_conf::<FRTBDataSet>(conf_path, mm)
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
        let df = DataFrame::new(
            seriess
                .into_iter()
                .map(|x| py_series_to_rust_series(x.as_ref(py)))
                .collect::<PyResult<Vec<Series>>>()?,
        )
        .map_err(PyUltimaErr::Polars)?;
        let source = DataSource::InMemory(df);
        let build_params = build_params.unwrap_or_default();
        let mm = bespoke_measures
            .unwrap_or_default()
            .into_iter()
            .map(|x| {
                let m = x._inner;
                (m.name().clone(), m)
            })
            .collect::<MeasuresMap>();
        from_source::<DataSetBase>(py, source, measures, build_params, mm)
    }

    #[classmethod]
    fn frtb_from_frame(
        _: &PyType,
        py: Python,
        seriess: Vec<Py<PyAny>>,
        measures: Option<Vec<String>>,
        build_params: Option<BTreeMap<String, String>>,
        bespoke_measures: Option<Vec<MeasureWrapper>>,
    ) -> PyResult<Self> {
        let df = DataFrame::new(
            seriess
                .into_iter()
                .map(|x| py_series_to_rust_series(x.as_ref(py)))
                .collect::<PyResult<Vec<Series>>>()?,
        )
        .map_err(PyUltimaErr::Polars)?;
        let source = DataSource::InMemory(df);
        let build_params = build_params.unwrap_or_default();
        let mm = bespoke_measures
            .unwrap_or_default()
            .into_iter()
            .map(|x| {
                let m = x._inner;
                (m.name().clone(), m)
            })
            .collect::<MeasuresMap>();
        from_source::<FRTBDataSet>(py, source, measures, build_params, mm)
    }

    #[classmethod]
    fn from_source(
        _: &PyType,
        py: Python,
        source: DataSourceWrapper,
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
        from_source::<DataSetBase>(py, source.inner, measures, build_params, mm)
    }

    pub fn ui(&self, py: Python) -> PyResult<()> {
        let a = Arc::clone(&self.dataset);
        py.allow_threads(|| a.ui());
        Ok(())
    }

    pub fn prepare(&mut self, collect: Option<bool>) -> PyResult<()> {
        let mut ds = self.dataset.write().expect("Poisonned RwLock");
        ds.prepare().map_err(PyUltimaErr::Ultima)?;
        if let Some(true) = collect {
            ds.collect().map_err(PyUltimaErr::Ultima)?;
        }
        Ok(())
    }

    pub fn compute(
        &self,
        py: Python,
        request: requests::ComputeRequestWrapper,
    ) -> PyResult<Vec<PyObject>> {
        py.allow_threads(|| {
            self.dataset
                .read()
                .expect("Poisonned RwLock")
                .compute(request.ar)
                .map_err(PyUltimaErr::Ultima)?
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
    pub fn frame(&self, fltrs: Option<Vec<Vec<FilterWrapper>>>) -> PyResult<Vec<PyObject>> {
        let fltrs = if let Some(f) = fltrs {
            f.into_iter()
                .map(|inner| {
                    inner
                        .into_iter()
                        .map(|fltr_wrap| fltr_wrap.inner)
                        .collect::<Vec<FilterE>>()
                })
                .collect::<Vec<Vec<FilterE>>>()
        } else {
            Default::default()
        };

        self.dataset
            .read()
            .expect("Poisonned RwLock")
            .get_lazyframe(&fltrs)
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
            .get_schema()
            .map_err(PyUltimaErr::Ultima)?;

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

    pub fn validate(&self, subset: Option<u8>) -> PyResult<()> {
        let subset = if let Some(u) = subset { u } else { 0 };
        self.dataset
            .read()
            .expect("Poisonned RwLock")
            .validate_frame(None, subset)
            .map_err(PyUltimaErr::Ultima)?;

        Ok(())
    }
}
