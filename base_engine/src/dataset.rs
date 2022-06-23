use polars::prelude::*;


/// Main source of data for FRTBRequest
/// pointer to/copy of FRTBDataSet to be passed with each request
//#[derive(Clone)]
pub struct DataSet {
    pub f1: DataFrame,
    pub f2: DataFrame,
    pub f3: DataFrame,
    pub numeric_cols: Vec<String>, //@TODO: cast to f64 upon build
}

pub trait DataSetT{
    fn columns_owned(&self, buf: Vec<String>) -> Vec<String>;
    fn numeric_columns_owned(&self, buf: Vec<String>) -> Vec<String>;
    fn validate(&self);
    //@TODO
    //fn filter();
    //fn prepare(); - pre calculate 
}

impl DataSetT for DataSet{

    /// Returns all avaiiable columns in the Dataset
    fn columns_owned(&self, mut buf: Vec<String>) -> Vec<String> {
        for df in [&self.f1, &self.f2, &self.f3] {
            let cn = df.get_column_names_owned();
            for i in cn {
                buf.push(i)
            }
        };
        // Convert to hash set?
        buf.sort_unstable();
        buf.dedup();
        buf
    }

    ///Numeric columns(these + bespoke are potential measures)
    fn numeric_columns_owned(&self, mut buf: Vec<String>) -> Vec<String> {
        for c in self.columns_owned(vec![]){
            match self.f1.column(&c) {
                Ok(s) => { match is_numeric(s){
                    true => {buf.push(c);} ,
                    false => continue,
                } },
                Err(_) => {
                    match self.f2.column(&c) {
                        Ok(s) => { match is_numeric(s){
                            true =>{ buf.push(c); },
                            false => continue,
                        } },
                        Err(_) => match self.f3.column(&c) {
                            Ok(s) => { match is_numeric(s){
                                true =>{ buf.push(c); },
                                false => continue,
                            } },
                            Err(_) => continue,
                        },
                    };   
                },
            };
        };
        buf
    }

    /// Validate Dataset contains columns 
    /// files_join_attributes and attributes_join_hierarchy
    /// numeric_cols and @todo dimensions(groups and filters)
    /// !only! numeric_col can be a measure
    ///  and therefore <numeric_col> should be only one across DataSet
    /// see how to validate dtype:
    /// https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    fn validate(&self) {}
}

pub fn is_numeric(s: &Series) -> bool {
    match s.dtype() {
        DataType::Utf8 | DataType::List(_) | DataType::Boolean | DataType::Null | DataType::Categorical(_) => false,
        _ => true,
    }
}

#[cfg(feature = "FRTB")]
impl frtb_engine::FRTBDataSetT for DataSet {
    //prebuild() - pre calculate common columns such as ..SensWeighted
}
