use polars::prelude::*;
use log::{warn, debug, info};
use serde::{Serialize, Deserialize};

use crate::dataset::*;

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
        #[serde(default)]
        f1_numeric_cols: Vec<String>,
        #[serde(default)]
        f2_numeric_cols: Vec<String>,
        #[serde(default)]
        f3_numeric_cols: Vec<String>,
    }
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
                attributes_join_hierarchy: a2h,
                f1_numeric_cols,
                f2_numeric_cols,
                f3_numeric_cols,} => {

                    let mut df1 = match  file_1 {
                        Some(y) => path_to_df(y, f2a,f1_numeric_cols),
                        _ => empty_frame(f2a) };

                    let mut df2 = match  v{
                        Some(y) => path_to_df(y, f2a, f2_numeric_cols),
                        _ => empty_frame(f2a) };

                    let mut df3 = match  c{
                        Some(y) => path_to_df(y, f2a, f3_numeric_cols),
                        _ => empty_frame(f2a) };
                    
                    let mut tmp = f2a.clone();
                    tmp.extend(a2h.clone());
                    let mut df_attr = match  ta{
                        Some(y) => path_to_df(y, &tmp, &[])
                                            .unique(Some(f2a), UniqueKeepStrategy::First).unwrap(),
                        _ => {
                            empty_frame(&tmp) 
                        }};
                    
                    //here we expect if hms is provided then a2h is not empty
                    let mut df_hms = match  hms{
                        Some(y) =>{ path_to_df(y, a2h, &[])
                                            .unique(Some(a2h), UniqueKeepStrategy::First)
                                            .expect("hms file path was provided, hence attributes_join_hierarchy list must also be provided
                                            in the datasource_config.toml") },
                        _ => empty_frame(a2h) };

                    // Cast to Categorical, needed for Join later
                    // Set a global string cache
                    // https://docs.rs/polars/0.13.3/polars/docs/performance/index.html
                    use polars::toggle_string_cache;
                    toggle_string_cache(true);

                    for i in a2h {
                        df_hms.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df_attr.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        
                    }
                    for i in f2a {
                        df_attr.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df1.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df2.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df3.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();

                    }

                    let f2a = f2a.to_vec();

                    // join with hms if a2h was provided
                    if !a2h.is_empty() {
                        df_attr = df_attr.join(&df_hms, a2h.clone(), a2h.clone(), JoinType::Left, None).unwrap();
                    }
                    df1 = df1.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    df2 = df2.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    df3 = df3.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    
                    let mut numeric_cols = f1_numeric_cols.to_owned();
                    numeric_cols.extend(f2_numeric_cols.clone());
                    numeric_cols.extend(f3_numeric_cols.clone());
        
                    DataSet{f1: df1, f2: df2, f3: df3, numeric_cols}
                },
        }
    }
}

fn empty_frame (with_columns: &[String]) -> DataFrame {
    let mut x: Vec<Series> = Vec::with_capacity(with_columns.len());
    let y: [String; 0] = [];
    for c in with_columns {
        x.push(Series::new(c, &y));
    }
    DataFrame::new_no_checks(x)
    //DataFrame::default()
}

/// reads DataFrame from path, casts cols to str and numeric cols to f64
fn path_to_df(path: &str, cast_to_str: &[String], cast_to_f64: &[String]) -> DataFrame {
    debug!("Reading: {}", path);
    // if path provided, then we expect it to be of the correct format
    // unrecoverable. Panic if failed to read file
    let mut df = CsvReader::from_path(path)
        .unwrap()
        .has_header(true)
        .finish()
        .unwrap();

    let mut _df: &mut DataFrame = &mut df;
    for i in cast_to_str {
        _df = match _df.try_apply(i, |s| 
            s.cast(&DataType::Utf8) ).ok() {
                Some(x) => x,
                // unrecoverable error
                None => panic!("Column {i} provided in datasource_config.toml must be present in {path}")
            }
        }
    
    
    // normalize numeric columns by casting to f64 and filling Null with 0 
    for c in _df.get_columns_mut() {
        if is_numeric(c) {
            *c = c.cast(&DataType::Float64)
                //.and_then(|s|
                //    s.fill_null(FillNullStrategy::Zero))
                .unwrap();
        }
    }
    
    df
}