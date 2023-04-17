use crate::calculator::CalculatorWrapper;
use crate::filter::FilterWrapper;
use pyo3::types::PyType;
use pyo3::{pyclass, pymethods};
use ultibi::filters::fltr_chain;
use ultibi::filters::FilterE;
use ultibi::BaseMeasure;
use ultibi::DependantMeasure;
use ultibi::Measure;

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
        //lambda: PyObject,
        //output_type: Wrap<DataType>,
        //inputs: Vec<String>,
        //returns_scalar: bool,
        precompute_filter: Vec<Vec<FilterWrapper>>,
        aggregation_restriction: Option<String>,
    ) -> Self {
        /*         let exprs = inputs.iter().map(|name| col(name)).collect::<Vec<_>>();

               let output = GetOutput::from_type(output_type.0);

               // Convert function into Expr
               let calculator = move |op: &CPM| {
                   let l = lambda.clone();
                   let params = op.clone();

                   Ok(apply_multiple(
                       move |s: &mut [Series]| {
                           let ll = l.clone();
                           let args = params.clone();

                           Python::with_gil(move |py| {
                               // this is a python Series
                               let out = call_lambda_with_args_and_series_slice(py, &args, s, &ll);

                               // we return an error, because that will become a null value polars lazy apply list
                               if out.is_none(py) {
                                   return Ok(None);
                               }
                               let srs = py_series_to_rust_series(out.as_ref(py)).ok(); // convert Res to Option

                               Ok(srs)
                           })
                       },
                       exprs.clone(),
                       output.clone(),
                       returns_scalar,
                   ))
               };

               let calculator = Arc::new(calculator);
        */
        let precompute_filters = precompute_filter
            .into_iter()
            .map(|or| {
                or.into_iter()
                    .map(|fltr| fltr.inner)
                    .collect::<Vec<FilterE>>()
            })
            .collect::<Vec<Vec<FilterE>>>();

        let precomputefilter = fltr_chain(&precompute_filters);

        let inner: Measure = BaseMeasure {
            name,
            calculator: calc.inner,
            precomputefilter,
            aggregation: aggregation_restriction,
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
    ) -> Self {
        let inner: Measure = DependantMeasure {
            name,
            calculator: calc.inner,
            depends_upon,
        }
        .into();

        Self { _inner: inner }
    }
}
