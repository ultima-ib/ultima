pub mod prelude;


use serde::{Serialize, Deserialize};
use polars::prelude::*;
use log::{warn, debug};
use lazy_static::lazy_static;

#[cfg(feature = "FRTB")]
use frtb_engine::prelude::*;

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
pub enum DataSourceConfig {
    CSV{#[serde(default, rename = "file_1_path")]
        file_1: Option<String>,
        #[serde(default, rename = "file_2_path")]
        file_2: Option<String>,
        #[serde(default, rename = "file_3_path")]
        file_3: Option<String>,
        #[serde(default, rename = "attributes_path")]
        attr: Option<String>,
        #[serde(default, rename = "hierarchy_path")]
        hms: Option<String>,
    }
}

/// Main source of data for FRTBRequest
/// pointer to/copy of FRTBDataSet to be passed with each request
#[derive(Clone)]
pub struct DataSet {
    pub f1: DataFrame,
    pub f2: DataFrame,
    pub f3: DataFrame,
    pub attr: DataFrame,
    pub hms: DataFrame
}

impl DataSourceConfig {
    /// build's and validates FRTBDataSet
    /// if path is None, returns empty DataFrame
    pub fn build_data(&self) -> DataSet {

        match self{
            DataSourceConfig::CSV{
                file_1: d, 
                file_2: v,
                file_3: c,
                attr: ta,
                hms} => {

                    let f1 = match  d {
                        Some(y) => path_to_df(y),
                        _ => empty_frame() };

                    let f2 = match  v{
                        Some(y) => path_to_df(y),
                        _ => empty_frame() };

                    let f3 = match  c{
                        Some(y) => path_to_df(y),
                        _ => empty_frame() };
                    
                    let attr = match  ta{
                        Some(y) => path_to_df(y),
                        _ => empty_frame() };
                    
                    let hms = match  hms{
                        Some(y) => path_to_df(y),
                        _ => empty_frame() };
        
                    DataSet{f1, f2, f3, attr, hms}
                },
        }
    }
}

fn empty_frame () -> DataFrame {
    let x: Vec<Series> = vec![];
    DataFrame::new(x).unwrap()
}

/// reads LazyFrame from path, validating schema
fn path_to_df(path: &str ) -> DataFrame {
    debug!("Reading: {}", path);
    let df = CsvReader::from_path(path)
                            .unwrap()
                            .has_header(true)
                            .finish()
                            // if path provided, then we expect it to be of the correct format, hence
                            // unrecoverable. Panic if failed to read file
                            .unwrap();
    df
}

/// this is FRTBRequest specific funtion
/// main function which returns a DataFrame
pub fn sa_capital(req: FRTBRequest, data: &DataSet, reg_config: FRTBRegParams) -> Result<DataFrame> { 
    // Step 0. Validate Filters
    // Step 1.2 or 1.0 .select takes a pointer to df, returning a new df
    // In lazy .select takes lf, returning a new lf
    // Hence to minimize copying would make sence to do .select first?
    // -> but .select depends on the measure+groupby+filters+file
    // Step 1.1 Apply Filters. Filters are file specific. Have to cast within filter
    // https://docs.rs/polars/0.13.3/polars/docs/performance/index.html
    // 1.3 pass two/three frames (eg delta, curv, hms) | merge, then pass(avoids merging for multiple measures)
    // 1.4 Groupby + calc measure

    // delta, vega, curv columns are fixed
    // hms, ta columns could be passed to avoid recomputing every time
    let (hms_cols, 
         ta_cols) = (
        data.hms.get_column_names_owned() ,
        data.attr.get_column_names_owned()   
        );
    

    let hms_required_cols: Vec<String> = Vec::from(["BookId".to_string()]);
    let mut hms = data.hms.select(vec!["BookId", "Desk", "Country", "LegalEntity"])?;
    
    println!("{:?}", hms);

    let (dlt, veg, crv, trd) =
    (data.f1.clone(), data.f2.clone(), data.f3.clone(), data.attr.clone());  

    let mut hms = hms.lazy();
    let dlt_lf = dlt.lazy();

    //Step 1.0 Applying Filters:

    // Inside single Filter expect first element(X) of (X ,.) to be from the same file
    // ie OR "ON" filters have to be on columns of the same file
    // ie if flitrs[0].0 is in hms_cols, then filters[1].0 is in hms_cols
    for f in req.filters.into_iter() {
        match f {
            Filter::In(fltrs) => {
                if hms_cols.contains(&fltrs[0].0) {
                    hms = hms.filter(fltr_in_or_builder(fltrs));
                };
            },
            Filter::On(fltrs) => {
                    if hms_cols.contains(&fltrs[0].0) {
                        hms = hms.filter(fltr_on_or_builder(fltrs));
                    };
                } ,
            Filter::Out(fltrs) => {
                if hms_cols.contains(&fltrs[0].0) {
                    hms = hms.filter(fltr_out_or_builder(fltrs));
                };
            },
        }
    }

    if cfg!(feature = "FRTB") {
        println!("#### !FRTB! ####")
    }

    Ok(hms.collect()?)
 }

fn fltr_in_or_builder(mut f: Vec<(String, Vec<String>)>) -> Expr {
    let (a, b) = f.remove(0);
    let s = Series::new("filter", b);
    let cat = true; // set true for now, later to be checked if col is among cat cols
    let mut e: Expr;
    if cat {
        e = col(&*a).cast(DataType::Categorical(None))
         .is_in( s.lit().cast(DataType::Categorical(None)) );
    } else { e = col(&*a) .is_in( s.lit()  ) };

    for (i, j) in f.into_iter() {
        let s = Series::new("filter", j);
        if cat {
            e = e.or( col(&*i).cast(DataType::Categorical(None))
            .is_in( s.lit().cast(DataType::Categorical(None)) ) );
        } else {
            e = e.or ( col(&*i) 
            .is_in( s.lit()  ))
        }
    } 
    e
} 

/// To add categorical here
fn fltr_on_or_builder(mut f: Vec<(String, String)>) -> Expr {
    let (a, b) = f.remove(0);
    let mut e: Expr = col(&*a).eq(lit::<String>(b));
    for (i, j) in f.into_iter() {
        e = e.or(col(&*i).eq(lit::<String>(j)))
    };
    e
}

/// To add categorical here
fn fltr_out_or_builder(mut f: Vec<(String, String)>) -> Expr {
    let (a, b) = f.remove(0);
    let mut e: Expr = col(&*a).neq(lit::<String>(b));
    for (i, j) in f.into_iter() {
        e = e.or(col(&*i).neq(lit::<String>(j)))
    };
    e
}

pub fn fx_delta (delta_hms_lf: LazyFrame) -> LazyFrame {
     // REMEMBER fx_delta + fx_curvature requires additional filter on RiskFactor
     //df.with_columns
     delta_hms_lf
 }


#[derive(Serialize, Deserialize, Debug)]
pub struct FRTBRequest { 
    // general fields
    measures: Vec<String>,
    groupby: Vec<String>,
    filters: Vec<Filter>,
    // FRTB Specific
    /// A spot rate must exist for translation from DeltaCcy
    /// into reporting_ccy
    reporting_ccy: ReportingCCY,
    cob: chrono::NaiveDate,
}

pub trait DataRequest{
    /// Returns a list of columns required (from DataSet)
    /// to perform calculation by looking into self.filters and self.groupby
    /// self.measures
    fn required_columns(&self) -> Vec<&str>;
}

impl DataRequest for FRTBRequest {
    fn required_columns(&self) -> Vec<&str> {
        let mut res: Vec<&str> = Vec::with_capacity(self.groupby.len());

        // each of the groupby cols must be present in the database
        for i in &self.groupby {
            res.push(i)
        }
        for f in &self.filters{
            match f{
                Filter::On(v) | Filter::Out(v) => { 
                    for s in v {
                        res.push(&s.0)
                }},
                Filter::In(v) => { 
                    for s in v {
                        res.push(&s.0)
                }},
            }
        }
        res
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum ReportingCCY{
    USD,
    EUR
}

/// Inner elements of each Filter are OR
/// Filters themselves are AND
/// Inside single Filter expect first element(X) of (X ,.) to be from the same file
/// ie OR "ON" filters have to be on columns of the same file
/// ie if flitrs[0].0 is in hms_cols, then filters[1].0 is in hms_cols
#[derive(Serialize, Deserialize, Debug)]
pub enum Filter {
    /// On Same as In, but better for 1 field only
    On(Vec<(String, String)>),
    In(Vec<(String, Vec<String>)>),
    Out(Vec<(String, String)>)
}

lazy_static!(
    static ref DELTA_COLUMNS: [String; 17] = [
    "TradeId".to_string(),
    "RiskClass".to_string(),
    "RiskFactor".to_string(),
    "RiskFactorType".to_string(), 
    "DeltaCcy".to_string(),
    "DeltaSpot".to_string(),
    "Delta0.25".to_string(),
    "Delta0.5Y".to_string(),
    "Delta1Y".to_string(),
    "Delta2Y".to_string(),
    "Delta3Y".to_string(),
    "Delta5Y".to_string(),
    "Delta10Y".to_string(),
    "Delta15Y".to_string(),
    "Delta20Y".to_string(),
    "Delta30Y".to_string(),
    "DeltaCcy".to_string()];
    static ref DELTA_CAT: Vec<String> = vec![
    "RiskClass".into(),
    "RiskFactor".into(),
    "RiskFactorType".into(),
    "DeltaCcy".into()];
    static ref DELTA_CAST: [Expr; 11] = [
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

impl Validate for DataSet{

    /// Helper function
    /// Used to access possible groupby/filters param in FRTBRequest from a FRTBDataSet
    /// every column of hms or ta file is a valid group/filter
    fn derive_groups(&self, mut buf: Vec<String>) -> Vec<String> {
        let cn = &self.hms.get_column_names();
        for i in cn {
            buf.push(i.to_string())
        };

        let cn = &self.attr.get_column_names();
        for i in cn {
            buf.push(i.to_string())
        };

        // Conver to hash set?
        buf.sort_unstable();
        buf.dedup();
        buf
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
