use crate::calculator::CalculatorWrapper;
use crate::filter::FilterWrapper;
use pyo3::types::PyType;
use pyo3::{pyclass, pymethods};
use ultibi::filters::fltr_chain;
use ultibi::filters::FilterE;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::{BaseMeasure, CalcParameter};

#[pyclass]
#[derive(Clone)]
pub struct CalcParamWrapper {
    pub _inner: CalcParameter,
}

#[pymethods]
impl CalcParamWrapper {
    #[new]
    fn new(name: String, default: Option<String>, type_hint: Option<String>) -> Self {
        let _inner = CalcParameter {
            name,
            default,
            type_hint,
        };
        Self { _inner }
    }
}
#[pyclass]
#[derive(Clone)]
pub struct MeasureWrapper {
    pub _inner: Measure,
}

#[pymethods]
impl MeasureWrapper {
    #[classmethod]
    fn new_basic(
        _: &PyType,
        name: String,
        calc: CalculatorWrapper,
        precompute_filter: Option<Vec<Vec<FilterWrapper>>>,
        aggregation_restriction: Option<String>,
        calc_params: Option<Vec<CalcParamWrapper>>,
    ) -> Self {
        let precompute_filters = precompute_filter
            .unwrap_or_default()
            .into_iter()
            .map(|or| {
                or.into_iter()
                    .map(|fltr| fltr.inner)
                    .collect::<Vec<FilterE>>()
            })
            .collect::<Vec<Vec<FilterE>>>();

        let precomputefilter = fltr_chain(&precompute_filters);

        let calc_params = calc_params
            .unwrap_or_default()
            .into_iter()
            .map(|cpw| cpw._inner)
            .collect::<Vec<CalcParameter>>();

        let inner: Measure = BaseMeasure {
            name,
            calculator: calc.inner,
            precomputefilter,
            aggregation: aggregation_restriction,
            calc_params,
        }
        .into();

        Self { _inner: inner }
    }

    #[classmethod]
    fn new_dependant(
        _: &PyType,
        name: String,
        calc: CalculatorWrapper,
        depends_upon: Vec<(String, String)>,
        calc_params: Option<Vec<CalcParamWrapper>>,
    ) -> Self {
        let calc_params = calc_params
            .unwrap_or_default()
            .into_iter()
            .map(|cpw| cpw._inner)
            .collect::<Vec<CalcParameter>>();

        let inner: Measure = DependantMeasure {
            name,
            calculator: calc.inner,
            depends_upon,
            calc_params,
        }
        .into();

        Self { _inner: inner }
    }
}
