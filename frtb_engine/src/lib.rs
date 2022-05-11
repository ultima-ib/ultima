//! FRTB job entry point
pub mod prelude;

use polars::export::arrow::array::Utf8Array;
use serde::{Serialize, Deserialize};
use polars::prelude::*;
use std::borrow::Borrow;
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
        trade_attributes: Option<String>,
        #[serde(default)]
        hms_path: Option<String>,
    }
}

// pointer to FRTBDataSet to be passed with each request
pub struct FRTBDataSet {
    pub delta: Option<LazyFrame>,
    pub vega: Option<LazyFrame>,
    pub curv: Option<LazyFrame>,
    pub trade_attr: Option<LazyFrame>,
    pub hms: Option<LazyFrame>
}

impl FRTBDataSet {
    pub fn new() -> Self {
        Self {delta: None, vega: None, curv: None, trade_attr: None, hms: None}
    }
}

impl FRTBDataSetUp {
    ///build's and validates FRTBDataSet
    pub fn build_data(&self) -> FRTBDataSet {

        match self{
            FRTBDataSetUp::CSV{delta_path: d, 
                vega_path: v,
                curvature_path: c,
                trade_attributes: ta,
                hms_path: hms} => {

                    let delta = match  d {
                        Some(y) => Some(path_to_lazy_df(y, Arc::clone(&DELTA_SCHEMA))),
                        _ => None };

                    let vega = match  v{
                        Some(y) => Some(path_to_lazy_df(y, Arc::clone(&VEGA_SCHEMA))),
                        _ => None };

                    let curv = match  c{
                        Some(y) => Some(path_to_lazy_df(y, Arc::clone(&VEGA_SCHEMA))),
                        _ => None };
                    
                    let trade_attr = match  ta{
                        Some(y) => Some(path_to_lazy_df(y, Arc::clone(&VEGA_SCHEMA))),
                        _ => None };
                    
                    let hms = match  hms{
                        Some(y) => Some(path_to_lazy_df(y, Arc::clone(&VEGA_SCHEMA))),
                        _ => None };
        
                    FRTBDataSet{delta, vega, curv, trade_attr, hms}
                },
            _ => todo!(),
        }
    }
}

/// reads LazyFrame from path, validating schema
fn path_to_lazy_df(path: &str, schema: Arc<Schema>) -> LazyFrame {
    debug!("Reading: {}", path);
    LazyCsvReader::new(path.into())
                            .has_header(true)
                            //.with_dtype_overwrite(schema)
                            //.with_schema(schema)
                            .finish()
                            .unwrap()
                            .with_columns(vec![
                                col("RiskClass").cast(DataType::Categorical(None)),
                                col("RiskFactorType").cast(DataType::Categorical(None)),
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
                                ])
                            //.collect()/
}

/// main function which returns a LazyFrame
pub fn sa_capital(req: FRTBRequest, data: FRTBDataSet, reg_config: FRTBRegParams){ 
    
 }

 pub fn fx_delta (delta_df: LazyFrame) -> LazyFrame {
     //df.with_columns
     delta_df
 }


#[derive(Serialize, Deserialize, Debug)]
pub struct FRTBRequest {
    // source: String, 
    cob: chrono::NaiveDate,
    measures: Vec<String>,
    groupby: Vec<String>, // To be replaced by polars Expr?
    reporting_ccy: ReportingCCY,
    filters: Vec<FRTBFilter>,
}

#[derive(Serialize, Deserialize, Debug)]
enum ReportingCCY{
    USD,
    EUR
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FRTBFilter {
    On(String, String),
    Out(String, String)
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
    //static ref RiskClassArray: Utf8Array<i64> = Utf8Array::<i64>::from([Some("FX"), Some("GIRR")]);
    
    //static ref rc: RevMapping = RevMapping::Local(Utf8Array::<i64>::from([Some("FX"), Some("GIRR")]));

    static ref SUPPORTED_MEASURES: Vec<String> = vec!["FX_Delta".into()];

    static ref DELTA_SCHEMA: Arc<Schema> = Arc::new(Schema::from(vec![
        Field::new("TradeId", DataType::Utf8),
        //Field::new("RiskClass", DataType::Categorical(
        //    Some(Arc::new(
        //        RevMapping::Local(Utf8Array::<i64>::from([Some("FX"), Some("GIRR")]))
        //        )
        //    )
        //)),
        Field::new("DeltaRiskFactor", DataType::Categorical(None)),
        Field::new("RiskFactorType", DataType::Categorical(None)),
        //Field::new("RiskClass", DataType::Utf8),
        Field::new("DeltaRiskFactor", DataType::Utf8),
        Field::new("RiskFactorType", DataType::Utf8),
        Field::new("Delta", DataType::Utf8),
        Field::new("DeltaCcy", DataType::Utf8),
    ]));

    static ref VEGA_SCHEMA: Arc<Schema> = Arc::new(Schema::from(vec![
        Field::new("TradeId", DataType::Utf8),
        Field::new("RiskClass", DataType::Categorical(
            Some(Arc::new(
                RevMapping::Local(Utf8Array::<i64>::from([Some("FX"), Some("GIRR")]))
                )
            )
        )),
        Field::new("DeltaRiskFactor", DataType::Categorical(None)),
        Field::new("RiskFactorType", DataType::Categorical(None)),
        Field::new("Delta", DataType::List(Box::new(DataType::Float64))),
        Field::new("DeltaCcy", DataType::Utf8),
    ]));

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
