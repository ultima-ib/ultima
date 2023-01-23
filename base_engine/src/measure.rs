//use derivative::Derivative;
use polars::prelude::{Expr, col};
//use serde::Serialize;
use std::collections::BTreeMap;

/// (Measure Name, Measure)
pub type MeasuresMap = BTreeMap<String, Measure>;
/// Optional Calculation Parameters
pub type OCP = BTreeMap<String, String>;

//type Calculator = Box<dyn Fn(&OCP) -> Expr + Send + Sync>;
type Calculator = Box<dyn Fn(Vec<Option<String>>) -> Expr + Send + Sync>;


/// Measure is the essentially a Struct of a funtion and a name
pub struct Measure {
    pub name: String,
    /// Main function which performs the calculation
    /// Note: Measures assumes presence of these columns
    pub calculator: Calculator,
    /// parameters which will go into calculator
    pub calc_params: Vec<String>,

    ///this field is to restrict aggregation option to certain type only
    ///for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'static str>,

    /// Optional
    /// Say you want to compute CSR Delta by Bucket
    ///
    /// You are only interested in CSR Buckets, all other would be 0,
    /// So we want to avoid unnecessary calculations.
    ///
    /// This field is an optional filter on DataFrame, placed PRIOR to the computation
    pub precomputefilter: Option<Expr>,

    /// Optional: Useful for caching, a measure can depend on other measures
    /// In which case other measures values could be used to derive
    /// result instead of recomputing from scratch
    pub depends_upon: Vec<Box<Measure>>,

    /// Optional: Used to identify how measure depends on depends_upon measures
    /// TODO: This potentially to be converted into another Calculator to allow ANY
    /// dependency type
    pub dependency_type: DependencyType
}

impl Default for Measure {
    fn default() -> Measure {
        Measure {
            name: "Default".into(),
            calculator: Box::new(|_: Vec<Option<String>>| col("*")),
            calc_params: vec![],
            aggregation: None,
            precomputefilter: None,
            depends_upon: vec![],
            dependency_type: DependencyType::None,
        }
    }
}

#[derive(Default)]
pub enum DependencyType {
    MAX,
    SUM,
    #[default]
    None
}

pub fn derive_basic_measures_vec(dataset_numer_cols: Vec<String>) -> Vec<Measure> {
    dataset_numer_cols
        .iter()
        .map(|x| {
            let y = x.clone();
            Measure {
                name: x.clone(),
                calculator: Box::new(move |_| col(y.as_str())),
                aggregation: None,
                precomputefilter: None,
                ..Default::default()
            }
        })
        .collect::<Vec<Measure>>()
}

pub fn derive_measure_map(measures_vecs: Vec<Measure>) -> MeasuresMap {
    let mut measure_map: MeasuresMap = BTreeMap::default();
    for m in measures_vecs {
        measure_map.insert(m.name.to_string(), m);
    }
    measure_map
}
