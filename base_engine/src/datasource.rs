use std::collections::HashMap;

use log::warn;
use polars::functions::diag_concat_df;
use polars::prelude::*;

use serde::{Deserialize, Serialize};

use crate::{dataset::*, derive_basic_measures_vec, Measure};

/// reads setup.toml
/// # Panics
/// When path or file is invalid
pub fn read_toml2<T>(path: &str) -> std::result::Result<T, Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned,
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
        }
        Err(er) => {
            warn!("Can't read file{}: {}", path, er);
            Err(er.into()) // convert std::io::error into Box dyn Error
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum DataSourceConfig {
    CSV {
        #[serde(default, rename = "files")]
        file_paths: Vec<String>,
        #[serde(default, rename = "attributes_path")]
        attr: Option<String>,
        #[serde(default, rename = "hierarchy_path")]
        hms: Option<String>,
        #[serde(default)]
        files_join_attributes: Vec<String>,
        #[serde(default)]
        attributes_join_hierarchy: Vec<String>,
        #[serde(default)]
        measures: Vec<String>,
        #[serde(default)]
        f1_numeric_cols: Vec<String>,
        #[serde(default)]
        f1_cast_to_str: Vec<String>,
        /// parameters to be used for build and prepare
        #[serde(default)]
        build_params: HashMap<String, String>,
    },
}

impl DataSourceConfig {
    /// build's and validates FRTBDataSet
    ///
    /// Returns:
    ///
    /// (joined concatinated DataFrame, vec of base measures, build params)
    pub fn build(self) -> (DataFrame, Vec<Measure>, HashMap<String, String>) {
        match self {
            DataSourceConfig::CSV {
                file_paths: files,
                attr: ta,
                hms,
                files_join_attributes: f2a,
                attributes_join_hierarchy: a2h,
                measures,
                f1_cast_to_str: mut str_cols,
                f1_numeric_cols: f64_cols,
                build_params,
            } => {
                // what if str_cols already contains f2a?
                str_cols.extend(f2a.clone());

                let mut concatinated_frame = diag_concat_df(
                    &files
                        .iter()
                        .map(|f| path_to_df(f, &str_cols, &f64_cols))
                        .collect::<Vec<DataFrame>>(),
                )
                .expect("Failed to concatinate provided frames"); // <- Ok to panic upon server startup

                let mut tmp = str_cols.clone();
                tmp.extend(a2h.clone());

                let mut df_attr = match ta {
                    Some(y) => path_to_df(&y, &tmp, &f64_cols)
                        .unique(Some(&f2a), UniqueKeepStrategy::First)
                        .unwrap(),
                    _ => empty_frame(&tmp),
                };

                //here we expect if hms is provided then a2h is not empty
                let df_hms = match  hms{
                        Some(y) =>{ path_to_df(&y, &a2h, &[])
                                            .unique(Some(&a2h), UniqueKeepStrategy::First)
                                            .expect("hms file path was provided, hence attributes_join_hierarchy list must also be provided
                                            in the datasource_config.toml") },
                        _ => empty_frame(&a2h) };

                // join with hms if a2h was provided
                if !a2h.is_empty() {
                    let a2h_expr = a2h.iter().map(|c|col(c)).collect::<Vec<Expr>>();
                    df_attr = df_attr.lazy()
                        .join(df_hms.lazy(), a2h_expr.clone(), a2h_expr, JoinType::Left)
                        .collect()
                        .expect("Could not join attributes to hms. Review attributes_join_hierarchy field in the setup");
                }
                // if files to attributes was provided
                if !f2a.is_empty() {
                    let f2a_expr = f2a.iter().map(|c|col(c)).collect::<Vec<Expr>>();
                    concatinated_frame = concatinated_frame.lazy()
                        .join(df_attr.lazy(), f2a_expr.clone(), f2a_expr, JoinType::Outer)
                        .collect()
                        .expect("Could not join files with attributes-hms. Review files_join_attributes field in the setup");
                }

                // if measures were provided
                let measures = if !measures.is_empty() {
                    // Checking if each measure is present in DF
                    measures.iter().for_each(|col| {
                        concatinated_frame
                            .column(col)
                            .unwrap_or_else(|_| panic!("Column {} not found", col));
                    });
                    derive_basic_measures_vec(measures)
                }
                // If not provided return all numeric columns
                else {
                    let num_cols = numeric_columns(&concatinated_frame);
                    derive_basic_measures_vec(num_cols)
                };

                (concatinated_frame, measures, build_params)
            }
        }
    }
}

fn empty_frame(with_columns: &[String]) -> DataFrame {
    let mut x: Vec<Series> = Vec::with_capacity(with_columns.len());
    let y: [String; 0] = [];
    for c in with_columns {
        x.push(Series::new(c, &y));
    }
    DataFrame::new_no_checks(x)
}

/// reads DataFrame from path, casts cols to str and numeric cols to f64
fn path_to_df(path: &str, cast_to_str: &[String], cast_to_f64: &[String]) -> DataFrame {
    let mut vc = Vec::with_capacity(cast_to_str.len() + cast_to_f64.len());
    for str_col in cast_to_str {
        vc.push(Field::new(str_col, DataType::Utf8))
    }
    for f64_col in cast_to_f64 {
        vc.push(Field::new(f64_col, DataType::Float64))
    }

    let schema = Schema::from(vc);

    // if path provided, then we expect it to be of the correct format
    // unrecoverable. Panic if failed to read file
    let df = LazyCsvReader::new(path)
        .has_header(true)
        .with_parse_dates(true)
        .with_dtype_overwrite(Some(&schema))
        //.with_ignore_parser_errors(ignore)
        .finish()
        .and_then(|lf| lf.collect())
        .unwrap_or_else(|_| panic!("Error reading file: {path}"));

    df
}
