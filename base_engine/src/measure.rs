use polars::prelude::*;
use std::collections::HashMap;
use derivative::Derivative;
use serde::{Serialize, Deserialize};

//MeasureMap
pub type MM<'b> = HashMap<String, Measure<'b>>;

/// Measure is the 
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Measure<'a>{
    pub name: String,
    /// Main function which performs the calculation
    #[derivative(Debug="ignore")]
    pub calculator: Box<dyn Fn(&Option<CalcParams>)->Expr + Send + Sync + 'a>,
    /// Bespoke measures assumes presence of these columns
    /// This field is used to validate the DataSet
    /// Req Columns of all availiable measures must be present in the DataSet
    //  pub req_columns: Vec<&'a str>, <-> not needed. We need a better way to validate DataSet.
    // perhaps through an optional build param
    ///this field is to restrict aggregation option to certain type only
    ///for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'a str>,
    // List of files
    //pub file: Vec<File>,
    //TODO cache
    // Measure to store cached requests and results
}

pub fn derive_basic_measures_vec<'a> (dataset_numer_cols: Vec<String>) -> Vec<Measure<'a>> {
    dataset_numer_cols
    .iter()
    .map(|x|{
        let y = x.clone();
        Measure {
            name: x.clone(),
            calculator: Box::new(move |_| col(y.as_str())),
            aggregation: None,
        }}
    )
    .collect::<Vec<Measure>>()
}

pub fn derive_measure_map<'a> (measures_vecs: Vec<Measure<'a>>)
 -> MM {
    let mut measure_map: MM = HashMap::default();
    for m in measures_vecs {
        measure_map
        .insert( m.name.to_string(), m );
    };
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
pub type Override = Vec< (String, String, Vec<(String, String)>) >;
pub type AddRows = Vec<String>;
pub type CalcParams = HashMap<String, String>;
pub type OCP = Option<CalcParams>;