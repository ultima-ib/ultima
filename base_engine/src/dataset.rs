use std::collections::HashMap;

use polars::prelude::*;

use crate::{derive_measure_map, DataSourceConfig, MM};

/// This is the default Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Debug)]
pub struct DataSetBase<'a> {
    pub frame: DataFrame,
    pub measures: MM<'a>,
    pub build_params: HashMap<String, String>,
}

/// The main Trait
/// 
/// If you have your own DataSet, implement this
pub trait DataSet {
    fn frame(&self) -> &DataFrame;
    fn measures(&self) -> &MM;
    fn build(conf: DataSourceConfig) -> Self;
    // These methods could be overwritten.
    /// Prepare runs ONCE before server starts.
    /// Any computations which are common to most queries could go in here.
    fn prepare(&mut self) {}
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    fn validate(&self) {}
}

impl<'a> DataSet for DataSetBase<'a> {
    fn frame(&self) -> &DataFrame {
        &self.frame
    }
    fn measures(&self) -> &MM {
        &self.measures
    }

    fn build(conf: DataSourceConfig) -> Self {
        let (frames, measure_cols, build_params) = conf.build();
        let mm: MM = derive_measure_map(measure_cols);
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
        if is_numeric(c) {
            res.push(c.name().to_string())
        }
    }
    res
}

pub fn is_numeric(s: &Series) -> bool {
    !matches!(
        s.dtype(),
        DataType::Utf8
            | DataType::List(_)
            | DataType::Boolean
            | DataType::Null
            | DataType::Categorical(_)
    )
}
