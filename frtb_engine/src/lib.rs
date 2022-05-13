//! FRTB job entry point
pub mod prelude;

use polars::export::arrow::array::Utf8Array;
use serde::{Serialize, Deserialize};
use polars::prelude::*;
use std::{borrow::Borrow, collections::HashMap};
use std::fs::File;
use std::io::Read;
use std::str::FromStr;
//use std::error::Error;
//use std::result::Result;
use log::{warn, debug};
use lazy_static::lazy_static;
use yearfrac::DayCountConvention;

/// reads setup.toml 
/// # Panics
/// When path or file is invalid
pub fn read_toml2<'de, T>(path: &'de str) -> std::result::Result<T, Box<dyn std::error::Error>>
where T: serde::de::DeserializeOwned,
 {
    
    let result_string: std::result::Result<String, std::io::Error> = std::fs::read_to_string(path);


    match result_string {
        Ok(f) => {
            let x = toml::from_str::<T>(&f);
            match x {
                Ok(obj) => Ok(obj),
                Err(er) => {
                    warn!("File {} found, but can't parse the file: {}", path, er);
                    Err(er.into()) // convert toml de::error into Box dyn Error
                }
            }
        },
        Err(er) => {
            warn!("Can't read file{}: {}", path, er);
            Err(er.into()) // convert std::io::error into Box dyn Error
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "paths")]
pub enum FRTBDataSetUp {
    CSV{#[serde(default)]
        delta_path: Option<String>,
        #[serde(default)]
        vega_path: Option<String>,
        #[serde(default)]
        curvature_path: Option<String>,
        #[serde(default)]
        trade_attributes_path: Option<String>,
        #[serde(default)]
        hms_path: Option<String>,
    }
}

/// Main source of data for FRTBRequest
/// pointer to/copy of FRTBDataSet to be passed with each request
#[derive(Clone)]
pub struct FRTBDataSet {
    pub delta: Option<DataFrame>,
    pub vega: Option<DataFrame>,
    pub curv: Option<DataFrame>,
    pub trade_attr: Option<DataFrame>,
    pub hms: Option<DataFrame>
}

impl FRTBDataSet {
    pub fn new() -> Self {
        Self {delta: None, vega: None, curv: None, trade_attr: None, hms: None}
    }
}

impl FRTBDataSetUp {
    /// build's and validates FRTBDataSet
    /// if path is None, returns empty DataFrame
    pub fn build_data(&self) -> FRTBDataSet {

        match self{
            FRTBDataSetUp::CSV{delta_path: d, 
                vega_path: v,
                curvature_path: c,
                trade_attributes_path: ta,
                hms_path: hms} => {

                    let delta = match  d {
                        Some(y) => Some(path_to_lazy_df(y, &*DELTA_CAST)),
                        _ => None };

                    let vega = match  v{
                        Some(y) => Some(path_to_lazy_df(y, &*DELTA_CAST)),
                        _ => None };

                    let curv = match  c{
                        Some(y) => Some(path_to_lazy_df(y, &*DELTA_CAST)),
                        _ => None };
                    
                    let trade_attr = match  ta{
                        Some(y) => Some(path_to_lazy_df(y, &[])),
                        _ => None };
                    
                    let hms = match  hms{
                        Some(y) => Some(path_to_lazy_df(y, &[])),
                        _ => None };
        
                    FRTBDataSet{delta, vega, curv, trade_attr, hms}
                },
            _ => todo!(),
        }
    }
}

/// reads LazyFrame from path, validating schema
fn path_to_lazy_df(path: &str, schema: &[Expr] ) -> DataFrame {
    debug!("Reading: {}", path);
    let h = LazyCsvReader::new(path.into())
                            .has_header(true)
                            .with_ignore_parser_errors(false)
                            //.with_dtype_overwrite(schema)
                            //.with_schema(schema)
                            .finish()
                            // if path provided, then we expect it to be of the correct format, hence
                            // unrecoverable. Panic if failed to read file
                            .unwrap()
                            .with_columns(schema)
                            .collect()
                            .unwrap();
    debug!("DF: {:?}", h);
    h
    
}

/// main function which returns a LazyFrame
pub fn sa_capital(req: FRTBRequest, data: FRTBDataSet, reg_config: FRTBRegParams) -> Result<DataFrame> { 
    // Step 0. Validate Filters
    // Step 1.1 Cast Categorical since has to be casted within same lazy query
    // https://docs.rs/polars/0.13.3/polars/docs/performance/index.html
    // Step 1.2 Apply Filters. Filters are file specific.
    // Need to know which filter applies to which file | unless join full set first BUT that's expensive.
    // 1.3 pass two/three frames (eg delta, curv, hms) | merge, then pass(avoids merging for multiple measures)
    // 1.4 Groupby + calc measure


    // if data: &FRTBDataSet
    //let mut dlt = data.delta.as_ref().and_then(|x| Some( ( (*x).clone(), (*x).get_column_names_owned())) );
    //let veg = data.vega.as_ref().and_then(|x| Some((*x).clone()) );
    //let crv = data.curv.as_ref().and_then(|x| Some((*x).clone()) );
    //let mut hms = data.hms.as_ref().and_then(|x|  Some( ( (*x).clone().lazy(), (*x).get_column_names_owned())) );
    //let trd = data.trade_attr.as_ref().and_then(|x|  Some( ( (*x).clone(), (*x).get_column_names())) );
  
    print!("HMS: {:?}", data.hms);
    // delta, vega, curv columns are fixed
    // hms, ta columns could be passed to avoid recomputing every time
    let (mut dlt_cols,
         mut vega_cols, 
         mut curv_cols, 
         mut hms_cols, 
         mut ta_cols) = 
    (data.delta.as_ref().and_then(|x| Some( (*x).get_column_names_owned() ) ) ,
    data.vega.as_ref().and_then(|x| Some( (*x).get_column_names_owned() ) ),
    data.curv.as_ref().and_then(|x| Some( (*x).get_column_names_owned() ) ),
    data.hms.as_ref().and_then(|x| Some( (*x).get_column_names_owned() ) ),
    data.trade_attr.as_ref().and_then(|x| Some( (*x).get_column_names_owned() ) ),   
    );
    
    let (dlt, veg, crv, hms, trd) =
     (data.delta, data.vega, data.curv, data.hms, data.trade_attr);

    //Step 1.0 Applying Filters:
    for f in req.filters {
        match f {
            FRTBFilter::In(fltrs) => (),
            FRTBFilter::On(fltrs) => {
                if let Some(ref mut y) = hms_cols {
                    if y.contains(&fltrs[0].0) {
                        println!("INSIDE A FILTER")
                    };
                    ()
                } else{
                ()}
            },
            FRTBFilter::Out(fltrs) => (),
        }
    }
    //let k = dlt.unwrap().0.clone().lazy();   
    //let k = filter_in(k, "RiskClass", vec!["FX".to_string()], true);

    let h = hms.unwrap();

    Ok(h)
 }

pub fn filter_in(lf: LazyFrame, field: &str, _in: Vec<String>, cat: bool ) -> LazyFrame {
    let s = Series::new("filter", _in);
    if cat {
        lf
        .filter( col(field).cast(DataType::Categorical(None)) .is_in( s.lit().cast(DataType::Categorical(None)) ))
    } else {
        lf
        .filter( col(field) .is_in( s.lit() ) ) 
    }
} 

pub fn filter_on() {}

pub fn fx_delta (delta_hms_lf: LazyFrame) -> LazyFrame {
     // REMEMBER fx_delta + fx_curvature requires additional filter on RiskFactor
     //df.with_columns
     delta_hms_lf
 }


#[derive(Serialize, Deserialize, Debug)]
pub struct FRTBRequest { 
    cob: chrono::NaiveDate,
    measures: Vec<String>,
    groupby: Vec<String>, // To be replaced by polars Expr?
    /// A spot rate must exist for translation from DeltaCcy
    /// into reporting_ccy
    reporting_ccy: ReportingCCY,
    filters: Vec<FRTBFilter>,
}

#[derive(Serialize, Deserialize, Debug)]
enum ReportingCCY{
    USD,
    EUR
}

/// Inner elements of each Filter are OR
/// Filters themselves are AND
#[derive(Serialize, Deserialize, Debug)]
pub enum FRTBFilter {
    /// On Same as In, but useful for 1 field only
    On(Vec<(String, String)>),
    In(Vec<(String, Vec<String>)>),
    Out(Vec<(String, String)>)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

lazy_static!(
    static ref DELTA_CAT: Vec<String> = vec!["RiskClass".into(), "RiskFactorType".into(), "DeltaCcy".into()];
    static ref DELTA_CAST: [Expr; 11] = [
                                //col("RiskClass").cast(DataType::Categorical(None)),
                                //col("RiskFactorType").cast(DataType::Categorical(None)),
                                col("DeltaSpot").cast(DataType::Float64),
                                col("Delta0.25Y").cast(DataType::Float64),
                                col("Delta0.5Y").cast(DataType::Float64),
                                col("Delta1Y").cast(DataType::Float64),
                                col("Delta2Y").cast(DataType::Float64),
                                col("Delta3Y").cast(DataType::Float64),
                                col("Delta5Y").cast(DataType::Float64),
                                col("Delta10Y").cast(DataType::Float64),
                                col("Delta15Y").cast(DataType::Float64),
                                col("Delta20Y").cast(DataType::Float64),
                                col("Delta30Y").cast(DataType::Float64),
                                //col("DeltaCcy").cast(DataType::Categorical(None)),
                                ];

    static ref SUPPORTED_MEASURES: Vec<String> = vec!["FX_Delta".into()];
/* Not supported yet
    static ref DELTA_SCHEMA: Arc<Schema> = Arc::new(Schema::from(vec![
        Field::new("TradeId", DataType::Utf8),
        Field::new("RiskClass", DataType::Categorical(
            Some(Arc::new(
                RevMapping::Local(Utf8Array::<i64>::from([Some("FX"), Some("GIRR")]))
                )
            )
        )),
        Field::new("DeltaRiskFactor", DataType::Categorical(None)),
        Field::new("RiskFactorType", DataType::Categorical(None)),
        //Field::new("RiskClass", DataType::Utf8),
        Field::new("DeltaRiskFactor", DataType::Utf8),
        Field::new("RiskFactorType", DataType::Utf8),
        Field::new("Delta", DataType::Utf8),
        Field::new("DeltaCcy", DataType::Utf8),
    ]));
*/
); 

#[derive(Serialize, Deserialize, Debug)]
pub struct FRTBRegParams {
    #[serde(default="true_func")]
    girr_rw_div: bool, // 21.44
    #[serde(default="true_func")]
    fx_rw_div: bool, // 21.88
    #[serde(default="usd_func")]
    reporting_ccy: String
}

fn true_func() -> bool {
    true
}

fn usd_func() -> String {
    "USD".into()
}

pub trait Validate{
    fn derive_groups(&self, buf: Vec<String>) -> Vec<String>;
}

impl Validate for FRTBDataSet{

    /// Helper function
    /// Used to access possible groupby/filters param in FRTBRequest from a FRTBDataSet
    /// every column of hms or ta file is a valid group/filter
    fn derive_groups(&self, mut buf: Vec<String>) -> Vec<String> {
        if let Some(ref hms) = &self.hms {
            let cn = hms.get_column_names();
            for i in cn {
                buf.push(i.to_string())
            }
        } ;

        if let Some(ref ta) = &self.trade_attr {
            let cn = ta.get_column_names();
            for i in cn {
                buf.push(i.to_string())
            }
        } ;

        // Conver to hash set?
        buf.sort_unstable();
        buf.dedup();
        buf
    }
}

struct Wrapper(String);
use std::fmt;

/*
impl fmt::Display for Wrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
*/

use std::ops::Deref;

impl Deref for Wrapper {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0 //pointer to LazyFrame
    }
}

impl Wrapper {
    fn say_hello(&self) {
        println!("HELLO")
    }
}
