use std::collections::HashMap;

use polars::prelude::*;

use crate::{DataSourceConfig, MM, derive_measure_map};

/// This is the default Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Debug)]
pub struct DataSetBase<'a> {
    pub frames: Vec<DataFrame>,
    pub measures: MM<'a>, 
    pub build_params: HashMap<String, String>,
}

/// The main Trait
/// If you have your own DataSet, implement this
pub trait DataSet{
    fn frames(&self) -> &Vec<DataFrame>;
    fn measures(&self) -> &MM;
    fn build(conf: DataSourceConfig) -> Self;

    fn columns_owned(&self, mut buf: Vec<String>) -> Vec<String> {
        for df in self.frames() {
            let cn = df.get_column_names_owned();
            for i in cn {
                buf.push(i)
            }
        };
        buf.sort_unstable();
        buf.dedup();
        buf
    }

    ///Numeric columns
    fn numeric_columns_owned(&self, mut buf: Vec<String>) -> Vec<String> {
        for df in self.frames() {
            for c in df.get_columns() {
                match is_numeric(c) {
                    true => buf.push(c.name().to_string()),
                    false => continue,
                }
            }
        }
        buf.sort_unstable();
        buf.dedup();
        buf
    }
    // These methods could be overwritten.
    /// Prepare runs ONCE before server starts.
    /// Any computations which are common to most queries could go in here.
    fn prepare(&mut self) {}
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    fn validate(&self) {}
    
}

impl<'a> DataSet for DataSetBase<'a>{
    fn frames(&self) -> &Vec<DataFrame>{
        &self.frames
    }
    fn measures(&self) -> &MM{
        &self.measures
    }

    fn build(conf: DataSourceConfig) -> Self{
        let (frames, measure_cols, build_params) = conf.build();
        let mm: MM = derive_measure_map(measure_cols);
        Self{frames, measures: mm, build_params}
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

impl<'a> DataSetBase<'a> {
    pub fn f1(&self) -> &DataFrame {
        // Polars DataFrame clones are super cheap:
        //https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
        &self.frames()[0]
    }
}

pub (crate) fn numeric_columns(df: &DataFrame) -> Vec<String> {
    let mut res = vec![];
    for c in df.get_columns() {
        if is_numeric(c) {
            res.push(c.name().to_string())
        }
    };
    res
}

pub fn is_numeric(s: &Series) -> bool {
    match s.dtype() {
        DataType::Utf8 | DataType::List(_) | DataType::Boolean | DataType::Null | DataType::Categorical(_) => false,
        _ => true,
    }
}
