use std::collections::HashMap;

use polars::prelude::*;
use serde::{Serializer, Serialize, ser::SerializeMap};

use crate::{derive_measure_map, DataSourceConfig, MeasuresMap};

/// This is the default Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Debug, Default)]
pub struct DataSetBase {
    pub frame: DataFrame,
    pub measures: MeasuresMap,
    pub build_params: HashMap<String, String>,
}

/// The main Trait
/// 
/// If you have your own DataSet, implement this
pub trait DataSet: Send + Sync {
    fn frame(&self) -> &DataFrame;
    fn measures(&self) -> &MeasuresMap;
    fn build(conf: DataSourceConfig) -> Self where Self: Sized;
    // These methods could be overwritten.
    /// Prepare runs ONCE before server starts.
    /// Any computations which are common to most queries could go in here.
    fn prepare(&mut self) {}
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    fn validate(&self) -> bool {true}
}

impl<'a> DataSet for DataSetBase {
    fn frame(&self) -> &DataFrame {
        &self.frame
    }
    fn measures(&self) -> &MeasuresMap {
        &self.measures
    }

    fn build(conf: DataSourceConfig) -> Self {
        let (frames, measure_cols, build_params) = conf.build();
        let mm: MeasuresMap = derive_measure_map(measure_cols);
        Self {
            frame: frames,
            measures: mm,
            build_params,
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

//pub(crate) fn utf8_columns(df: &DataFrame) -> Vec<String> {
//    let mut res = vec![];
//    for c in df.get_columns() {
//        if let DataType::Utf8 = c.dtype() {
//            res.push(c.name().to_string())
//        }
//    }
//    res
//}

pub(crate) fn utf8_columns_unique_vals(df: &DataFrame) -> PolarsResult<HashMap<String, Vec<Option<String>>>> {
    let mut res = HashMap::new();
    for c in df.get_columns() {
        if let DataType::Utf8 = c.dtype() {
            res.insert(c.name().to_string(),
             c.unique()?.utf8()?.into_iter()
                .map(|x|
                    x.map(|y|y.to_string())).collect::<Vec<Option<String>>>());
        }
    }
    Ok(res)
}

impl Serialize for dyn DataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let df = self.frame();
        let measures = self.measures()
            .iter()
            .map(|(x, m)| (x, m.aggregation))
            .collect::<HashMap<&String, Option<&str>>>();

        let col_map = utf8_columns_unique_vals(df)
            .map_err(|_|serde::ser::Error::custom("Could not serialize column"))?;

        let mut seq = serializer.serialize_map(Some(2))?;
        seq.serialize_entry("fields", &col_map)?;
        seq.serialize_entry("measures", &measures)?;
        seq.end()
    }
}
