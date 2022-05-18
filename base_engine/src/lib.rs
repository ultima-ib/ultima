pub mod prelude;

use serde::{Serialize, Deserialize};
use polars::prelude::*;
use log::{warn, debug, info};

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
        #[serde(default)]
        files_join_attributes: Vec<String>,
        #[serde(default)]
        attributes_join_hierarchy: Vec<String>,
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
    pub hms: DataFrame,
    pub f2a: Vec<String>,
    pub a2h: Vec<String>,
}

impl DataSourceConfig {
    /// build's and validates FRTBDataSet
    /// if path is None, returns empty DataFrame
    pub fn build_data(&self) -> DataSet {

        match self{
            DataSourceConfig::CSV{
                file_1, 
                file_2: v,
                file_3: c,
                attr: ta,
                hms,
                files_join_attributes: f2a,
                attributes_join_hierarchy: a2h} => {

                    let f1 = match  file_1 {
                        Some(y) => path_to_df(y),
                        _ => empty_frame(f2a) };

                    let f2 = match  v{
                        Some(y) => path_to_df(y),
                        _ => empty_frame(f2a) };

                    let f3 = match  c{
                        Some(y) => path_to_df(y),
                        _ => empty_frame(f2a) };
                    
                    let attr = match  ta{
                        Some(y) => path_to_df(y),
                        _ => {
                            let mut tmp = f2a.clone();
                            tmp.extend(a2h.clone());
                            empty_frame(&tmp) 
                        }};
                    
                    let hms = match  hms{
                        Some(y) => path_to_df(y),
                        _ => empty_frame(a2h) };
                    
                    info!("{:?}", f1);
                    info!("{:?}", f2);
                    info!("{:?}", attr);
                    info!("{:?}", hms);

                    let f2a = f2a.to_vec();
                    let a2h = a2h.to_vec();
        
                    DataSet{f1, f2, f3, attr, hms, f2a, a2h}
                },
        }
    }
}

fn empty_frame (with_columns: &Vec<String>) -> DataFrame {
    let mut x: Vec<Series> = Vec::with_capacity(with_columns.len());
    for c in with_columns {
        x.push(Series::new(c, &[""]));
    }
    DataFrame::new(x).unwrap()
}

/// reads DataFrame from path
fn path_to_df(path: &str ) -> DataFrame {
    debug!("Reading: {}", path);
    // if path provided, then we expect it to be of the correct format
    // unrecoverable. Panic if failed to read file
    let df = CsvReader::from_path(path)
        .unwrap()
        .has_header(true)
        .finish()
        .unwrap();
    df
}

/// main function which returns a DataFrame
pub fn execute(req: DataRequestS, data: &DataSet) -> Result<DataFrame> { 
    // Step 0. Validate Filters
    // Step 1.2 or 1.0 .select takes a pointer to df, returning a new df
    // In lazy .select takes lf, returning a new lf
    // Hence to minimize copying would make sence to do .select first?
    // -> but .select depends on the measure+groupby+filters+file
    // Step 1.1 Apply Filters. Filters are file specific. Have to cast within filter
    // https://docs.rs/polars/0.13.3/polars/docs/performance/index.html
    // 1.3 pass two/three frames (eg delta, curv, hms) | merge, then pass(avoids merging for multiple measures)
    // 1.4 Groupby + calc measure
    // 1.5 Before calc measure check if it exists in cache 

    // Step 1.0 Select columns required for current request

    // from dataset, minimizing cloning
    let (f1_cols, f2_cols, f3_cols,
        hms_cols, attr_cols) = (
        data.f1.get_column_names(),
        data.f2.get_column_names(),
        data.f3.get_column_names(),
        data.hms.get_column_names(),
        data.attr.get_column_names()   
        );
    
    
    let mut f1_select: Vec<&str> = vec![]; 
    let mut f2_select: Vec<&str> = vec![]; 
    let mut f3_select: Vec<&str> = vec![]; 
    let mut attr_select: Vec<&str> = vec![]; 
    let mut hms_select: Vec<&str> = vec![]; 

    for i in &data.f2a {
        f1_select.push(&i);
        f2_select.push(&i);
        f3_select.push(&i);
        attr_select.push(&i);
    }
    for i in &data.a2h {
        attr_select.push(&i);
        hms_select.push(&i);
    }

    let req_cols = req.required_columns();
    println!("Required columns: {:?}", req_cols);

    for col in req_cols {

        if f1_cols.contains(&col) {
            f1_select.push(col)
        } 

        if f2_cols.contains(&col) {
            f2_select.push(col)
        } 
        
        if f3_cols.contains(&col) {
            f3_select.push(col)
        } 
        
        if hms_cols.contains(&col) {
            hms_select.push(col)
        } 
        
        if attr_cols.contains(&col) {
            attr_select.push(col)
        }
    };
    
    let mut hms = data.hms.select(hms_select)?
        .lazy();

    //Step 1.1 Applying Filters:

    // Inside single Filter expect first element(X) of (X ,.) to be from the same file
    // ie OR "ON" filters have to be on columns of the same file
    // ie if flitrs[0].0 is in hms_cols, then filters[1].0 is in hms_cols
    for f in req.filters {
        match f {
            FilterS::In(fltrs) => {
                if hms_cols.contains(&&*fltrs[0].0) {
                    hms = hms.filter(fltr_in_or_builder(fltrs));
                };
            },
            FilterS::Eq(fltrs) => {
                if hms_cols.contains(&&*fltrs[0].0) {
                    hms = hms.filter(fltr_on_or_builder(fltrs));
                };
                } ,
            FilterS::Neq(fltrs) => {
                if hms_cols.contains(&&*fltrs[0].0) {
                    hms = hms.filter(fltr_out_or_builder(fltrs));
                };
            },
        }
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
    println!("a: {}, b: {}", a, b);
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
pub struct DataRequestS { 
    // general fields
    /// Measure can be of two types:
    /// basic: Column - Action
    /// bespoke: DerivedField - Action
    measures: Vec<(String, String)>,
    groupby: Vec<String>,
    filters: Vec<FilterS>,
    // FRTB Specific
    /// A spot rate must exist for translation from DeltaCcy
    /// into reporting_ccy
    reporting_ccy: ReportingCCY, // <- potentially to go into FRTB hyper params?
    // cob: chrono::NaiveDate <-- will be part of the filters 
}

pub trait DataRequestT{
    /// Returns a list of columns required (from DataSet)
    /// to perform calculation by looking into self.filters and self.groupby
    /// self.measures
    fn required_columns(&self) -> Vec<&str>;
}

impl DataRequestT for DataRequestS {
    fn required_columns(&self) -> Vec<&str> {
        let mut res: Vec<&str> = Vec::with_capacity(self.groupby.len() + self.filters.len());

        // each of the groupby cols must be present
        for i in &self.groupby {
            res.push(i)
        }
        // each filter column must be present
        for f in &self.filters{
            match f{
                FilterS::Eq(v) | FilterS::Neq(v) => { 
                    for s in v {
                        res.push(&s.0)
                }},
                FilterS::In(v) => { 
                    for s in v {
                        res.push(&s.0)
                }},
            }
        }

        // each measure must be present
        for m in &self.measures {
            // check if the measure is one of bespoke measures
            if cfg!(feature = "FRTB") {
                match frtb_engine::MEASURE_COL_MAP.get(&m.0){
                    Some(x) => { res.extend(x); },
                    _ => (),
                }
            } // if not assume it's a base measure and therefore
            // a column must be present
            else {
                res.push(&m.0);
            }
        };
        res.sort_unstable();
        res.dedup();
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
pub enum FilterS {
    /// On Same as In, but better for 1 field only
    Eq(Vec<(String, String)>),
    In(Vec<(String, Vec<String>)>),
    Neq(Vec<(String, String)>)
}

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
    fn columns_owned(&self, buf: Vec<String>) -> Vec<String>;
    fn validate(&self);
}

impl Validate for DataSet{

    /// Helper function
    /// Used to access possible groupby/filters param in FRTBRequest from a FRTBDataSet
    /// every column of hms or ta file is a valid group/filter
    fn columns_owned(&self, mut buf: Vec<String>) -> Vec<String> {
        for df in [&self.hms, &self.attr, &self.f1, &self.f2, &self.f3] {
            let cn = df.get_column_names_owned();
            for i in cn {
                buf.push(i)
            }
        };
        // Conver to hash set?
        buf.sort_unstable();
        buf.dedup();
        buf
    }

    /// Validate Dataset contains columns 
    /// files_join_attributes and attributes_join_hierarchy
    fn validate(&self) {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
