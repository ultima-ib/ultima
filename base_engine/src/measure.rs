//use derivative::Derivative;
use polars::prelude::{Expr, col};
//use serde::Serialize;
use std::collections::BTreeMap;

pub type MeasureName = String;
/// (Measure Name, Measure)
pub type MeasuresMap = BTreeMap<MeasureName, Measure>;
/// Optional Calculation Parameters
pub type OCP = BTreeMap<String, String>;

//type Calculator = Box<dyn Fn(&OCP) -> Expr + Send + Sync>;
type Calculator = Box<dyn Fn(Vec<Option<String>>) -> Expr + Send + Sync>;

/// Measure is the essentially a Struct of a calculator and a name
pub struct BaseMeasure {
    pub name: MeasureName,
    /// Main function which performs the calculation
    /// Executed in .groupby().agg() context
    pub calculator: Calculator,

    //pub calculator2: Expr,
    /// parameters which will go into calculator
    pub calc_params: Vec<String>,
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
pub struct DependantMeasure {

    pub name: String,

    /// executed within .with_columns() context
    pub calculator: Calculator, // MAX, SUM...

    /// parameters which will go into calculator
    pub calc_params: Vec<String>,

    /// this field is to restrict aggregation option to certain type only
    /// for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'static str>,

    /// Vec<(Depends Upon Measure Name, Aggregation type)>
    /// eg vec![(FXDeltaCharge, scalar), (Sensitivity, mean)]
    pub depends_upon: Vec<(Measure, String)>
}


/// AggRequest --> execute -->  split DependantMeasure into BaseMeasure's (BaseMeasure leave as they are) --> execute_aggregation --> combine back into original request
/// make cahce default (ie not a feature)
/// do not change the OUTPUT of any existing .get_measures() - because user/client does not/should not care about what kind of measure they are calling
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
            Measure::Base(BaseMeasure{aggregation,..})|
            Measure::Dependant(DependantMeasure{aggregation,..}) => aggregation,
        }
    }
    
    pub fn name(&self) -> &MeasureName {
        match self {
            Measure::Base(BaseMeasure{name,..})|
            Measure::Dependant(DependantMeasure{name,..}) => name,
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
            calculator: Box::new(|_: Vec<Option<String>>| col("*")),
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
            Measure::Base(BaseMeasure{
                name: x.clone(),
                calculator: Box::new(move |_| col(y.as_str())),
                ..Default::default()

            })
        })
        .collect::<Vec<Measure>>()
}

// Consumes Measure vec, returns Measure map
//pub fn derive_measure_map(measures_vecs: Vec<Measure>) -> MeasuresMap {
//    let mut measure_map: MeasuresMap = BTreeMap::default();
//    for m in measures_vecs {
//        measure_map.insert(m.name(), m);
//    }
//    measure_map
//}
