//use derivative::Derivative;
use polars::prelude::{col, Expr, PolarsError, PolarsResult};
use serde::Serialize;
//use serde::Serialize;
use std::collections::BTreeMap;

use crate::{CPM, aggregations::AggregationMethod};

//pub type OCPM = BTreeMap<String, String>;

/// Each [DataSet] has measures accessed via get_measures()
/// This alias to represent a measure name, a unique string
pub type MeasureName = String;

/// (Measure Name, Measure)
pub type MeasuresMap = BTreeMap<MeasureName, Measure>;

//type Calculator = Box<dyn Fn(&OCP) -> Expr + Send + Sync>;
type Calculator = Box<dyn Fn(&CPM) -> PolarsResult<Expr> + Send + Sync>;

/// This struct is purely for DataSet descriptive purposes(for now).
/// Recall measure may take parameters in form of HashMap<paramName, paramValue>
/// This struct returns all possible paramNames for the given Dataset (for UI purposes only)
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct CalcParameter {
    pub name: &'static str,
    pub default: Option<&'static str>,
    pub type_hint: Option<&'static str>,
}

/// Measure is the essentially a Struct of a calculator and a name
//#[derive(Clone)]
pub struct BaseMeasure {
    pub name: MeasureName,
    /// Main function which performs the calculation
    /// Executed in .groupby().agg() context
    pub calculator: Calculator,

    // TODO find a nice way to attach calc_params to a measure
    // parameters which will go into calculator
    //pub calc_params: &'static [CalcParameter],
    /// Optional: this field is to restrict aggregation option to certain type only
    /// for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'static str>,
    /// Optional
    /// Say you want to compute CSR Delta by Bucket
    ///
    /// You are only interested in CSR Buckets, all other would be 0,
    /// So we want to avoid unnecessary calculations.
    ///
    /// This field is an optional filter on DataFrame, placed PRIOR to the computation
    pub precomputefilter: Option<Expr>,
}

//
//BaseMeasure (EXPR)
/// Dependant Measure cannot be computed directly. Instead it is broken down into it's parents
/// parents get executed, and then used to compute the DependantMeasure.
///
/// Useful for caching.
///
/// No precomputefilter - it is inherited from parents
//#[derive(Clone)]
pub struct DependantMeasure {
    pub name: MeasureName,

    /// executed within .with_columns() context
    pub calculator: Calculator, // MAX, SUM...

    /// parameters which will go into calculator
    // pub calc_params: Vec<String>,

    /// this field is to restrict aggregation option to certain type only
    /// for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'static str>,

    /// Vec<(Depends Upon Measure Name, Aggregation type)>
    /// eg vec![(FXDeltaCharge, scalar), (Sensitivity, mean)]
    pub depends_upon: Vec<(&'static str, &'static str)>,
}

/// AggRequest --> execute -->  split DependantMeasure into BaseMeasure's (BaseMeasure leave as they are) --> execute_aggregation --> combine back into original request
/// make cahce default (ie not a feature)
/// do not change the OUTPUT of any existing .get_measures() - because user/client does not/should not care about what kind of measure they are calling
//#[derive(Clone)]
pub enum Measure {
    /// A typical measure
    /// execute_aggregation .groupby().agg(X)
    Base(BaseMeasure),
    /// DependantMeasure depends on BaseMeasure
    /// Executed within .with_columns() context
    Dependant(DependantMeasure),
}

impl FromIterator<Measure> for BTreeMap<MeasureName, Measure> {
    fn from_iter<I>(v: I) -> Self
    where
        I: IntoIterator<Item = Measure>,
    {
        v.into_iter()
            .map(|measure| (measure.name().clone(), measure))
            .collect()
    }
}

impl Measure {
    pub fn aggregation(&self) -> &Option<&'static str> {
        match self {
            Measure::Base(BaseMeasure { aggregation, .. })
            | Measure::Dependant(DependantMeasure { aggregation, .. }) => aggregation,
        }
    }

    pub fn name(&self) -> &MeasureName {
        match self {
            Measure::Base(BaseMeasure { name, .. })
            | Measure::Dependant(DependantMeasure { name, .. }) => name,
        }
    }
    /*
    pub fn calc_params(&self) -> &Vec<String>{
        match self {
            Measure::Base(BaseMeasure{calc_params,..})|
            Measure::Dependant(DependantMeasure{calc_params,..}) => calc_params,
        }
    }
    pub fn calculator(&self) -> &Calculator {
        match self {
            Measure::Base(BaseMeasure{calculator,..})|
            Measure::Dependant(DependantMeasure{calculator,..}) => calculator,
        }
    }
    pub fn precomputefilter(&self) -> &Option<Expr> {
        match self {
            Measure::Base(BaseMeasure{precomputefilter,..}) => precomputefilter,
            Measure::Dependant(_) => &None,
        }
    }
    */
}

impl Default for BaseMeasure {
    fn default() -> BaseMeasure {
        BaseMeasure {
            name: "Default".into(),
            calculator: Box::new(|_: &CPM| Ok(col("*"))),
            //calc_params: &[],
            aggregation: None,
            precomputefilter: None,
        }
    }
}

pub fn derive_basic_measures_vec(dataset_numer_cols: Vec<String>) -> Vec<Measure> {
    dataset_numer_cols
        .iter()
        .map(|x| {
            let y = x.clone();
            Measure::Base(BaseMeasure {
                name: x.clone(),
                calculator: Box::new(move |_| Ok(col(y.as_str()))),
                ..Default::default()
            })
        })
        .collect::<Vec<Measure>>()
}

/// Convert requested measure into [ProcessedMeasure] measure by looking up from all_availiable_measures.
///
/// NOTE: if a measure, which was looked up from all_availiable_measures has a predefined AggExpression
/// then we override requested measure.
///
/// by mapping requested String to a map of all availiable measures
pub(crate) fn base_measure_lookup(
    requested_measures: &[(MeasureName, AggregationMethod)],
    all_availiable_measures: &MeasuresMap,
    op: &CPM,
) -> PolarsResult<Vec<ProcessedMeasure>> {
    let res = requested_measures.iter()
        .map(|(requested_measure, requested_action)| {

            // Lookup requested measure from all_availiable_measures by name
            let Some(Measure::Base(m)) = all_availiable_measures.get(requested_measure as &str) else {
                return Err(PolarsError::ComputeError(format!("No measure {requested_measure} exists for the dataset. Availiable measures are: {:?}",
                    all_availiable_measures.keys()).into()))
            };

            // If measure has predefined aggregation, check that requested aggregation matches it          
            if let Some(default_action) = m.aggregation {
                if default_action != requested_action {
                    return Err(PolarsError::ComputeError(format!("Measure {requested_measure} supports only {default_action} aggregation,
                    but {requested_action} requested").into()))
                }
            }

            // Lookup action from the list of supported actions
            let Some(act) = crate::api::aggregations::BASE_CALCS.get(requested_action.as_str()) else {
                return Err(PolarsError::ComputeError(format!("No action {requested_action} supported. Supported actions are: {:?}",
                crate::api::aggregations::BASE_CALCS.keys()).into()))
            };

            //let params: CPM = m.calc_params
            //    .iter()
            //    .map(|param|(param, op.get(param).cloned()))
            //    .collect::<BTreeMap<String, String>>();

            // apply action
            let (calculator, name) = act(
                    (m.calculator)(op)?,
                    requested_measure
                );

            Ok(
                ProcessedMeasure {
                    name,
                    calculator,
                    precomputefilter: m.precomputefilter.clone(),
                }
            )
            }
        )
        .collect::<PolarsResult<Vec<ProcessedMeasure>>>();

    res
}

/// Unlike main Measure struct, this structure holds final name, extended Expr(with aggregation)
/// and the precompute filter.
///
/// This is basically a "processed" measure
pub(crate) struct ProcessedMeasure {
    pub name: String,
    pub calculator: Expr,
    pub precomputefilter: Option<Expr>,
    // TODO: potentially use as key for cache + ID for dataset
}
