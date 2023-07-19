//! Ultibi IO operations
//! This potentially will be moved to a separate crate

pub mod acquire;
pub mod helpers;

#[cfg(feature = "aws_s3")]
pub mod awss3;

#[cfg(feature = "aws_s3")]
use polars::functions::diag_concat_df;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    datasource::{DataSource, SourceVariant},
    Measure,
};
use helpers::{empty_frame, finish, path_to_lf};

/// reads setup.toml
/// # Panics
/// When path or file is invalid
pub fn read_toml2<T>(path: &str) -> std::result::Result<T, Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned,
{
    let result_string = std::fs::read_to_string(dbg!(path))?;
    let res = toml::from_str::<T>(&result_string)?;
    Ok(res)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum DataSourceConfig {
    CSV {
        #[serde(default)]
        source_type: SourceVariant,
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
        build_params: BTreeMap<String, String>,
    },
    #[cfg(feature = "aws_s3")]
    AwsCSV {
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
        build_params: BTreeMap<String, String>,
    },
}

impl DataSourceConfig {
    /// build's DataSet
    ///
    /// Returns:
    ///
    /// (joined concatinated DataFrame, vec of base measures, build params)
    pub fn build(self) -> (DataSource, Vec<Measure>, BTreeMap<String, String>) {
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
                source_type,
            } => {
                for s in f2a.iter() {
                    if !str_cols.contains(s) {
                        str_cols.push(s.to_string())
                    }
                }

                let concatinated_frame = diag_concat_lf(
                    &files
                        .iter()
                        .map(|f| path_to_lf(f, &str_cols, &f64_cols))
                        .collect::<Vec<LazyFrame>>(),
                    true,
                    true,
                )
                .expect("Failed to concatinate provided frames"); // <- Ok to panic upon server startup

                let mut tmp = str_cols.clone();
                tmp.extend(a2h.clone());

                let df_attr = match ta {
                    Some(y) => path_to_lf(&y, &tmp, &f64_cols)
                        .unique(Some(f2a.clone()), UniqueKeepStrategy::First),
                    //.unwrap(),
                    _ => empty_frame(&tmp).lazy(),
                };

                //here we expect if hms is provided then a2h is not empty
                let df_hms = match hms {
                    Some(y) => path_to_lf(&y, &a2h, &[])
                        .unique(Some(a2h.clone()), UniqueKeepStrategy::First),
                    //.expect("hms file path was provided, hence attributes_join_hierarchy list must also be provided
                    //in the datasource_config.toml") },
                    _ => empty_frame(&a2h).lazy(),
                };

                finish(
                    a2h,
                    f2a,
                    measures,
                    df_attr,
                    df_hms,
                    concatinated_frame,
                    build_params,
                    source_type,
                )
            }
            #[cfg(feature = "aws_s3")]
            DataSourceConfig::AwsCSV {
                bucket,
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
                str_cols.extend(f2a.clone());
                let frames = awss3::multi_download(
                    bucket.as_str(),
                    &files.iter().map(|p| p.as_str()).collect::<Vec<&str>>(),
                    &str_cols,
                    &f64_cols,
                );

                let concatinated_frame =
                    diag_concat_df(&frames).expect("Failed to concatinate provided frames");
                let mut tmp = str_cols.clone();

                tmp.extend(a2h.clone());

                let df_attr = match ta {
                    Some(y) => {
                        awss3::multi_download(bucket.as_str(), &[y.as_str()], &tmp, &f64_cols)
                            .remove(0)
                    }
                    _ => empty_frame(&tmp),
                };

                //here we expect if hms is provided then a2h is not empty
                let df_hms = match hms {
                    Some(y) => {
                        awss3::multi_download(bucket.as_str(), &[y.as_str()], &a2h, &[]).remove(0)
                    }
                    _ => empty_frame(&a2h),
                };

                finish(
                    a2h,
                    f2a,
                    measures,
                    df_attr.lazy(),
                    df_hms.lazy(),
                    concatinated_frame.lazy(),
                    build_params,
                    // Currently AWS doesn't support Scan
                    SourceVariant::InMemory,
                )
            }
        }
    }
}
