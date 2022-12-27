use derivative::Derivative;
use polars::prelude::*;
use serde::Serialize;
use std::collections::HashMap;

/// (Measure Name, Measure)
pub type MeasuresMap = HashMap<String, Measure>;
/// Optional Calculation Parameters
pub type OCP = HashMap<String, String>;

type Calculator = Box<dyn Fn(&OCP) -> Expr + Send + Sync>;

/// Measure is the essentially a Struct of a funtion and a name
#[derive(Derivative, Serialize)]
#[derivative(Debug)]
pub struct Measure {
    pub name: String,
    /// Main function which performs the calculation
    /// Note: Measures assumes presence of these columns
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub calculator: Calculator,
    ///this field is to restrict aggregation option to certain type only
    ///for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'static str>,
    /// Say you want to compute CSR Delta by Bucket
    ///
    /// You are only interested in CSR Buckets, all other would be 0,
    /// So we want to avoid unnecessary calculations.
    ///
    /// This field is an optional filter on DataFrame, placed PRIOR to the computation
    #[serde(skip)]
    pub precomputefilter: Option<Expr>,
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
            }
        })
        .collect::<Vec<Measure>>()
}

pub fn derive_measure_map(measures_vecs: Vec<Measure>) -> MeasuresMap {
    let mut measure_map: MeasuresMap = HashMap::default();
    for m in measures_vecs {
        measure_map.insert(m.name.to_string(), m);
    }
    measure_map
}
