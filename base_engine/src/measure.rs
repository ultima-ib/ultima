use derivative::Derivative;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

//MeasureMap
pub type MM<'b> = HashMap<String, Measure<'b>>;
type Calculator<'a> = Box<dyn Fn(&Option<CalcParams>) -> Expr + Send + Sync + 'a>;

/// Measure is the
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Measure<'a> {
    pub name: String,
    /// Main function which performs the calculation
    #[derivative(Debug = "ignore")]
    pub calculator: Calculator<'a>,
    /// Bespoke measures assumes presence of these columns
    /// This field is used to validate the DataSet
    /// Req Columns of all availiable measures must be present in the DataSet
    //  pub req_columns: Vec<&'a str>, <-> not needed. We need a better way to validate DataSet.
    // perhaps through an optional build param
    ///this field is to restrict aggregation option to certain type only
    ///for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'a str>,
    /// Say you want to compute CSR Delta by Bucket
    ///
    /// You are only interested in CSR Buckets, all other would be 0,
    /// So we want to avoid unnecessary calculations.
    ///
    /// This field is an optional filter on DataFrame, placed PRIOR to the computation
    pub precomputefilter: Option<Expr>,
}

pub fn derive_basic_measures_vec<'a>(dataset_numer_cols: Vec<String>) -> Vec<Measure<'a>> {
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

pub fn derive_measure_map(measures_vecs: Vec<Measure>) -> MM {
    let mut measure_map: MM = HashMap::default();
    for m in measures_vecs {
        measure_map.insert(m.name.to_string(), m);
    }
    measure_map
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct OptParams {
    // Overrides
    #[serde(default)]
    pub over_ride: Option<Override>,
    //TODO Extends DataFrame
    #[serde(default)]
    pub add_rows: Option<AddRows>,
    // If this could be achieved with Filters
    // then shouldn't be needed
    // pub remove_rows //could just filter out trades based on Id
    #[serde(default)]
    pub hide_zeros: bool,

    // Parameters, passed to bespoke measures(ie calculation parameters)
    #[serde(default)]
    pub calc_params: OCP,
}

// Column to override, value, where Vec<(Column, Value)>
pub type Override = Vec<(String, String, Vec<(String, String)>)>;
pub type AddRows = Vec<String>;
pub type CalcParams = HashMap<String, String>;
pub type OCP = Option<CalcParams>;
