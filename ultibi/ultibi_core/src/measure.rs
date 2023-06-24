use derivative::Derivative;
//use derivative::Derivative;
use polars::prelude::{col, Expr, PolarsError, PolarsResult};
use serde::Serialize;
//use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    aggregations::{Aggregation, AggregationName},
    CPM,
};

/// Each [DataSet] has measures accessed via get_measures()
/// This alias to represent a measure name, a unique string
pub type MeasureName = String;

/// (Measure Name, Measure)
pub type MeasuresMap = BTreeMap<MeasureName, Measure>;

//type Calculator = Box<dyn Fn(&OCP) -> Expr + Send + Sync>;
pub type Calculator = Arc<dyn Fn(&CPM) -> PolarsResult<Expr> + Send + Sync>;

/// This struct is purely for DataSet descriptive purposes(for now).
/// Recall measure may take parameters in form of HashMap<paramName, paramValue>
/// This struct returns all possible paramNames for the given Dataset (for UI purposes only)
#[derive(Debug, Default, Clone, Serialize, Hash, PartialEq, Eq)]
pub struct CalcParameter {
    pub name: String,
    pub default: Option<String>,
    pub type_hint: Option<String>,
}

/// Measure is the essentially a Struct of a calculator and a name
#[derive(Clone)]
pub struct BaseMeasure {
    pub name: MeasureName,
    /// Main function which performs the calculation
    /// Executed in .groupby().agg() context
    pub calculator: Calculator,

    // TODO find a nice way to attach calc_params to a measure
    // parameters which will go into calculator
    // pub calc_params: &'static [CalcParameter],
    /// Optional: this field is to restrict aggregation option to certain type only
    /// for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<String>,

    /// Optional
    /// Say you want to compute CSR Delta by Bucket
    ///
    /// You are only interested in CSR Buckets, all other would be 0,
    /// So we want to avoid unnecessary calculations.
    ///
    /// This field is an optional filter on DataFrame, placed PRIOR to the computation
    pub precomputefilter: Option<Expr>,

    /// Calc params
    /// Will determine the list of Params in the UI
    pub calc_params: Vec<CalcParameter>,
}

/// Dependant Measure cannot be computed directly. Instead it is broken down into it's parents
/// parents get executed, and then used to compute the DependantMeasure.
///
/// Useful for caching.
///
/// No precomputefilter - it is inherited from parents
#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Clone)]
pub struct DependantMeasure {
    pub name: MeasureName,

    /// executed within .with_columns() context
    #[derivative(Debug = "ignore")]
    pub calculator: Calculator, // MAX, SUM...

    // parameters which will go into calculator
    // pub calc_params: Vec<String>,

    // this field is to restrict aggregation option to certain type only
    // for example where it makes sence to aggregate with "first" and not "sum"
    // must be one of BASE_CALCS
    // Currently every dep measure is "scalar", see [Measure::aggregation]
    // pub aggregation: Option<&'static str>,
    /// Vec<(Depends Upon Measure Name, Aggregation type)>
    /// eg vec![(FXDeltaCharge, scalar), (Sensitivity, mean)]
    pub depends_upon: Vec<(String, String)>,

    /// Calc params
    /// Will determine the list of Params in the UI
    pub calc_params: Vec<CalcParameter>,
}

/// AggRequest --> execute -->  split DependantMeasure into BaseMeasure's (BaseMeasure leave as they are) --> execute_aggregation --> combine back into original request
/// make cahce default (ie not a feature)
/// do not change the OUTPUT of any existing .get_measures() - because user/client does not/should not care about what kind of measure they are calling
#[derive(Clone)]
pub enum Measure {
    /// A typical measure
    /// execute_aggregation .groupby().agg(X)
    Base(BaseMeasure),
    /// DependantMeasure depends on BaseMeasure
    /// Executed within .with_columns() context
    Dependant(DependantMeasure),
}

impl From<BaseMeasure> for Measure {
    fn from(item: BaseMeasure) -> Self {
        Measure::Base(item)
    }
}

impl From<DependantMeasure> for Measure {
    fn from(item: DependantMeasure) -> Self {
        Measure::Dependant(item)
    }
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
use once_cell::sync::Lazy;
pub(crate) static SCALAR: Lazy<Option<String>> = Lazy::new(|| Some("scalar".into()));

impl Measure {
    pub fn aggregation(&self) -> &Option<String> {
        match self {
            Measure::Base(BaseMeasure { aggregation, .. }) => aggregation,
            Measure::Dependant(_) => &SCALAR,
        }
    }

    pub fn name(&self) -> &MeasureName {
        match self {
            Measure::Base(BaseMeasure { name, .. })
            | Measure::Dependant(DependantMeasure { name, .. }) => name,
        }
    }
    pub fn calc_params(&self) -> &Vec<CalcParameter> {
        match self {
            Measure::Base(BaseMeasure { calc_params, .. })
            | Measure::Dependant(DependantMeasure { calc_params, .. }) => calc_params,
        }
    }
    pub fn calculator(&self) -> &Calculator {
        match self {
            Measure::Base(BaseMeasure { calculator, .. })
            | Measure::Dependant(DependantMeasure { calculator, .. }) => calculator,
        }
    }
}

impl Default for BaseMeasure {
    fn default() -> BaseMeasure {
        BaseMeasure {
            name: "Default".into(),
            calculator: Arc::new(|_: &CPM| Ok(col("*"))),
            calc_params: vec![],
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
                calculator: Arc::new(move |_| Ok(col(y.as_str()))),
                ..Default::default()
            })
        })
        .collect::<Vec<Measure>>()
}

/// This is the main [Measure] processed, ie it holds the final name, final Expr(with aggregation)
/// And the precompute filter for BasicMeasure
pub(crate) enum ProcessedMeasure {
    /// A typical measure
    /// execute_aggregation .groupby().agg(X)
    Base(ProcessedBaseMeasure),
    // DependantMeasure depends on BaseMeasure
    // Executed within .with_columns() context
    Dependant(ProcessedDependantMeasure),
}

pub(crate) struct ProcessedBaseMeasure {
    pub name: String,
    pub calculator: Expr,
    pub precomputefilter: Option<Expr>,
}

pub(crate) struct ProcessedDependantMeasure {
    pub _name: String,
    pub _calculator: Expr,
}

/// Looks up [Measure] from all_availiable_measures
/// If a measure is [Measure::Dependant] then also looks up children
/// Preserves depth to help with execution of dependants which must be executed in order
pub(crate) fn agg_measure_lookup<'b, 'a: 'b>(
    requested_measures: &'b [(MeasureName, AggregationName)],
    all_availiable_measures: &'a MeasuresMap,
) -> PolarsResult<
    Vec<(
        (&'b MeasureName, &'b AggregationName),
        (&'a Measure, &'a Aggregation),
    )>,
> {
    let res = requested_measures.iter()
        .map(|(requested_measure, requested_action)| {

            // Lookup requested measure from all_availiable_measures by name
            let Some(looked_up_measure) = all_availiable_measures.get(requested_measure as &str) else {
                return Err(PolarsError::ComputeError(format!("No measure {requested_measure} exists for the dataset. Availiable measures are: {:?}",
                    all_availiable_measures.keys()).into()))
            };

            // If measure has predefined aggregation, check that requested aggregation matches it          
            if let Some(default_action) = looked_up_measure.aggregation() {
                if default_action != requested_action {
                    return Err(PolarsError::ComputeError(format!("Measure {requested_measure} supports only {default_action} aggregation,
                    but {requested_action} requested").into()))
                }
            }

            // Lookup action from the list of supported actions
            let Some(a) = crate::aggregations::BASE_CALCS.get(requested_action.as_str()) else {
                return Err(PolarsError::ComputeError(format!("No action {requested_action} supported. Supported actions are: {:?}",
                crate::aggregations::BASE_CALCS.keys()).into()))
            };

            match looked_up_measure {
                Measure::Base(_) => Ok(vec![(
                    (requested_measure, requested_action), (looked_up_measure, a))]
                ),
                Measure::Dependant(dm) =>{
                    // TODO 
                    let children = &dm.depends_upon;
                    // get children
                    let children_lookup =
                        agg_measure_lookup(children, all_availiable_measures)?;
                    // Not adding self because dependant measures to be expressed separately
                    //children_lookup.push(((requested_measure, requested_action), (looked_up_measure, a)));

                    Ok(children_lookup)

                }
            }
        }
        )
        .collect::<PolarsResult<Vec<Vec<((&'b MeasureName, &'b AggregationName), (&'a Measure, &'a Aggregation))>>>>()?;

    Ok(res.into_iter().flatten().collect())
}

/// Looks up [Measure] from all_availiable_measures
/// If a measure is [Measure::Dependant] then also looks up children
/// Preserves depth to help with execution of dependants which must be executed in order
/// Note Parent is last
pub(crate) fn lookup_dependants_with_depth<'b, 'a: 'b>(
    requested_measures: &'b [(MeasureName, AggregationName)],
    all_availiable_measures: &'a MeasuresMap,
) -> Vec<Vec<(&'a DependantMeasure, &'a Aggregation)>> {
    let mut this_level_dependants = vec![];
    let mut this_level_children = vec![];

    for (requested_measure, agg_name) in requested_measures.iter() {
        // TODO potentially break loop here instead of panic
        let looked_up_measure = all_availiable_measures
            .get(requested_measure as &str)
            .unwrap();
        let agg = crate::aggregations::BASE_CALCS
            .get(agg_name.as_str())
            .unwrap();

        if let Measure::Dependant(dm) = looked_up_measure {
            this_level_dependants.push((dm, agg));
            this_level_children.push(&dm.depends_upon);
        }
    }

    if !this_level_children.is_empty() {
        let this_level_children_flat: Vec<(String, String)> =
            this_level_children.into_iter().flatten().cloned().collect();
        let mut next_level_dependants =
            lookup_dependants_with_depth(&this_level_children_flat, all_availiable_measures);
        next_level_dependants.extend(vec![this_level_dependants]);
        next_level_dependants
    } else {
        vec![this_level_dependants]
    }
}

pub(crate) fn agg_measure_to_expr(
    measure: &Measure,
    agg: &Aggregation,
    op: &CPM,
) -> PolarsResult<ProcessedMeasure> {
    let calculator = agg.aggregate(
        (measure.calculator())(op)?, // Calling Calculator with Parameters, returns an Expr
        measure.name(),
    );

    let new_name = agg.new_name(measure.name());

    match measure {
        Measure::Base(m) => Ok(ProcessedMeasure::Base(ProcessedBaseMeasure {
            name: new_name,
            calculator,
            precomputefilter: m.precomputefilter.clone(),
        })),
        Measure::Dependant(_) => Ok(ProcessedMeasure::Dependant(ProcessedDependantMeasure {
            _name: new_name,
            _calculator: calculator,
        })),
    }
}
