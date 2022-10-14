use std::collections::{BTreeMap, HashMap};

use polars::prelude::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::{derive_measure_map, DataSourceConfig, MeasuresMap};

/// This is the default struct which implements Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Debug, Default)]
pub struct DataSetBase {
    pub frame: DataFrame,
    pub measures: MeasuresMap,
    /// build_params are used in .prepare()
    pub build_params: HashMap<String, String>,
    pub calc_params: Vec<CalcParameter>,
}

/// This struct is purely for DataSet descriptive purposes.
/// Recall measure may take parameters in form of HashMap<paramName, paramValue>
/// This struct returns all possible paramNames for the given Dataset (for UI purposes only)
#[derive(Debug, Default, Clone, Serialize)]
pub struct CalcParameter {
    pub name: String,
    pub default: Option<String>,
    pub type_hint: Option<String>,
}

/// The main Trait
///
/// If you have your own DataSet, implement this
pub trait DataSet: Send + Sync {
    fn frame(&self) -> &DataFrame;
    fn measures(&self) -> &MeasuresMap;
    fn build(conf: DataSourceConfig) -> Self
    where
        Self: Sized;
    // These methods could be overwritten.

    /// Prepare runs ONCE before server starts.
    /// Any computations which are common to most queries could go in here.
    fn prepare(&mut self) {}

    fn calc_params(&self) -> Vec<CalcParameter> {
        vec![]
    }
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    fn validate(&self) -> bool {
        true
    }
}

impl DataSet for DataSetBase {
    fn frame(&self) -> &DataFrame {
        &self.frame
    }
    fn measures(&self) -> &MeasuresMap {
        &self.measures
    }
    /// It's ok to clone. Function is only called upon serialisation, so very rarely
    fn calc_params(&self) -> Vec<CalcParameter> {
        self.calc_params.clone()
    }

    fn build(conf: DataSourceConfig) -> Self {
        let (frames, measure_cols, build_params) = conf.build();
        let mm: MeasuresMap = derive_measure_map(measure_cols);
        Self {
            frame: frames,
            measures: mm,
            build_params,
            calc_params: vec![],
        }
    }

    //    /// Validate Dataset contains columns
    //    /// files_join_attributes and attributes_join_hierarchy
    //    /// numeric_cols and TODO dimensions(groups and filters)
    //    /// !only! numeric_col can be a measure
    //    ///  and therefore <numeric_col> should be only one across DataSet
    //    /// see how to validate dtype:
    //    /// https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    //    fn validate(&self) {}
}

pub(crate) fn numeric_columns(df: &DataFrame) -> Vec<String> {
    let mut res = vec![];
    for c in df.get_columns() {
        if c.dtype().is_numeric() {
            res.push(c.name().to_string())
        }
    }
    res
}

pub(crate) fn utf8_columns(df: &DataFrame) -> Vec<String> {
    let mut res = vec![];
    for c in df.get_columns() {
        if let DataType::Utf8 = c.dtype() {
            res.push(c.name().to_string())
        }
    }
    res
}

/// DataTypes supported for overrides are defined in [overrides::string_to_lit]
pub(crate) fn overrides_columns(df: &DataFrame) -> Vec<String> {
    let mut res = vec![];
    for c in df.get_columns() {
        match c.dtype() {
            DataType::Utf8|DataType::Boolean|DataType::Float64 => res.push(c.name().to_string()),
            DataType::List(x)  => match x.as_ref(){
                DataType::Float64 => res.push(c.name().to_string()),
                _ => (),
            },
            _ => (),
        }
    }
    res
}

impl Serialize for dyn DataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        //let df = self.frame();
        let measures = self
            .measures()
            .iter()
            .map(|(x, m)| (x, m.aggregation))
            .collect::<HashMap<&String, Option<&str>>>();

        let ordered_measures: BTreeMap<_, _> = measures.iter().collect();
        let utf8_cols = utf8_columns(self.frame());
        let ovrrd_cols = overrides_columns(self.frame());
        let calc_params = self.calc_params();

        let mut seq = serializer.serialize_map(Some(4))?;

        seq.serialize_entry("fields", &utf8_cols)?;
        seq.serialize_entry("measures", &ordered_measures)?;
        seq.serialize_entry("calc_params", &calc_params)?;
        seq.serialize_entry("overrides", &ovrrd_cols)?;
        seq.end()
    }
}
