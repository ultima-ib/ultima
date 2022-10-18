use std::collections::HashMap;
use log::warn;
use polars::functions::diag_concat_df;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::Measure;
pub mod helpers;
use helpers::{path_to_df,empty_frame, finish};

#[cfg(feature="aws_s3")]
pub mod awss3;

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
#[non_exhaustive]
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
    #[cfg(feature = "aws_s3")]
    AwsCsv {
        bucket: String,
        #[serde(rename = "files")]
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

                let concatinated_frame = diag_concat_df(
                    &files
                        .iter()
                        .map(|f| path_to_df(f, &str_cols, &f64_cols))
                        .collect::<Vec<DataFrame>>(),
                )
                .expect("Failed to concatinate provided frames"); // <- Ok to panic upon server startup

                let mut tmp = str_cols.clone();
                tmp.extend(a2h.clone());

                let df_attr = match ta {
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

                finish(a2h, f2a, measures, df_attr, df_hms, concatinated_frame, build_params)
            },
            #[cfg(feature = "aws_s3")]
            DataSourceConfig::AwsCsv{
                bucket,
                file_paths: files,
                attr: ta,
                hms,
                files_join_attributes: f2a,
                attributes_join_hierarchy: a2h,
                measures,
                f1_cast_to_str: mut str_cols,
                f1_numeric_cols: f64_cols,
                build_params} => {
                    let frames = awss3::multi_download(bucket.as_str(), &files.iter().map(|p|p.as_str()).collect::<Vec<&str>>());
                    let concatinated_frame = diag_concat_df(
                        &frames
                    )
                    .expect("Failed to concatinate provided frames");
                    unimplemented!()
                },
        }
    }
}



