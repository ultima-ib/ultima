use std::collections::HashMap;

use polars::prelude::*;
use log::{warn, debug};
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
        f1_measure_cols: Option<Vec<String>>,
        #[serde(default)]
        f2_measure_cols: Option<Vec<String>>,
        #[serde(default)]
        f3_measure_cols: Option<Vec<String>>,
        #[serde(default)]
        f1_numeric_cols: Option<Vec<String>>,
        #[serde(default)]
        f1_cast_to_str: Option<Vec<String>>,
        /// parameters to be used for build and prepare
        #[serde(default)]
        build_params: Option<HashMap<String, String>>,
    }
}

impl DataSourceConfig {
    /// build's and validates FRTBDataSet
    /// if path is None, returns empty DataFrame
    pub fn build(self) -> DataSet {

        match self{
            DataSourceConfig::CSV{
                file_1, 
                file_2: v,
                file_3: c,
                attr: ta,
                hms,
                files_join_attributes: f2a,
                attributes_join_hierarchy: a2h,
                f1_measure_cols: f1_m,
                f2_measure_cols: f2_m,
                f3_measure_cols: f3_m,
                f1_cast_to_str: cast_to_str_vec,
                f1_numeric_cols,
                mut build_params} => {
                    // in order to convert to categorical, f2a columns have to be Utf8
                    let mut str_cols: Vec<String> = match cast_to_str_vec {
                        Some(v) => v.clone(),
                        _ => vec![]
                    };
                    str_cols.extend(f2a.clone());
                    let f64_cols: Vec<String> = match f1_numeric_cols {
                        Some(v) => v.clone(),
                        _ => vec![]
                    };


                    let mut df1 = match  file_1 {
                        Some(y) => path_to_df(&y, &str_cols, &f64_cols),
                        _ => empty_frame(&f2a) };

                    let mut df2 = match  v{
                        Some(y) => path_to_df(&y, &f2a, &f64_cols),
                        _ => empty_frame(&f2a) };

                    let mut df3 = match  c{
                        Some(y) => path_to_df(&y, &f2a, &f64_cols),
                        _ => empty_frame(&f2a) };
                    
                    
                    let mut tmp = f2a.clone();
                    tmp.extend(a2h.clone());
                    let mut df_attr = match  ta{
                        Some(y) => path_to_df(&y, &tmp, &[])
                                            .unique(Some(&f2a), UniqueKeepStrategy::First).unwrap(),
                        _ => {
                            empty_frame(&tmp) 
                        }};
                    
                    //here we expect if hms is provided then a2h is not empty
                    let mut df_hms = match  hms{
                        Some(y) =>{ path_to_df(&y, &a2h, &[])
                                            .unique(Some(&a2h), UniqueKeepStrategy::First)
                                            .expect("hms file path was provided, hence attributes_join_hierarchy list must also be provided
                                            in the datasource_config.toml") },
                        _ => empty_frame(&a2h) };

                    // Cast to Categorical, needed for Join later
                    // Set a global string cache
                    // https://docs.rs/polars/0.13.3/polars/docs/performance/index.html
                    use polars::toggle_string_cache;
                    toggle_string_cache(true);

                    for i in a2h.iter() {
                        df_hms.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df_attr.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        
                    }
                    for i in f2a.iter() {
                        df_attr.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df1.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df2.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();
                        df3.try_apply(i, |s| 
                            s.cast(&DataType::Categorical(None))).unwrap();

                    }

                    // join with hms if a2h was provided
                    if !a2h.is_empty() {
                        df_attr = df_attr.join(&df_hms, a2h.clone(), a2h.clone(), JoinType::Left, None).unwrap();
                    }
                    df1 = df1.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    df2 = df2.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    df3 = df3.join(&df_attr, f2a.clone(), f2a.clone(), JoinType::Left, None).unwrap();
                    
                    
                    let mut measure_cols = vec![];
                    for (o, df) in [(f1_m, &df1), 
                        (f2_m, &df2),  (f3_m, &df3)] {
                        match o {
                            //TODO Check here if each of f1_m is present in the df1
                            Some(measures) =>{ measure_cols.extend(measures.clone()); },
                            // If not provided return all columns
                            None =>{ measure_cols.extend(numeric_columns(df)); },
                        };

                    };

                    let build_params = 
                    if let Some(bp) = build_params.take() {
                        bp
                    } else {
                        HashMap::default()
                    };

                    DataSet{f1: df1, f2: df2, f3: df3, measure_cols, build_params}
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
}

/// reads DataFrame from path, casts cols to str and numeric cols to f64
fn path_to_df(path: &str, cast_to_str: &[String], cast_to_f64: &[String]) -> DataFrame {
    let mut cast_exprs: Vec<Expr> = cast_to_str.iter()
        .map(|x| col(x as &str).cast(DataType::Utf8))
        .collect();

    cast_to_f64.iter()
        .for_each(|x| cast_exprs.push(col(x as &str).cast(DataType::Float64)));

    // if path provided, then we expect it to be of the correct format
    // unrecoverable. Panic if failed to read file
    let mut df = LazyCsvReader::new(path.to_string())
        .has_header(true)
        //.with_schema(Arc::new(myschema))
        .finish()
        .unwrap()
        .with_columns(&cast_exprs)
        .collect()
        .unwrap();

    let mut _df: &mut DataFrame = &mut df;    
    
    // Not needed because user has to provide numeric columns in dataconfig toml
    // normalize numeric columns by casting to f64
    //for c in _df.get_columns_mut() {
    //    if is_numeric(c) {
    //        *c = c.cast(&DataType::Float64).unwrap();
    //    }
    //}
    
    df
}