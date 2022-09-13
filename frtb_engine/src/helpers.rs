#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::prelude::*;

use log::warn;
use ndarray::{prelude::*, Data};
use serde::Deserialize;

/// if CRR2 feature is not activated, this will return BCBS
/// if jurisdiction is not part of optional params or can't parse this will return BCBS
pub(crate) fn get_jurisdiction(op: &OCP) -> Jurisdiction {
    op.as_ref()
        .and_then(|map| map.get("jurisdiction"))
        .and_then(|x| x.parse::<Jurisdiction>().ok())
        //.unwrap()
        .unwrap_or_else(|| {
            warn!("Jurisdiction defaulted to: BCBS");
            Jurisdiction::default()
        })
}

/// Limited to types which implement Copy because this should only be used for types like f64
pub(crate) fn get_optional_parameter<'a, T>(op: &'a OCP, param: &str, default: &T) -> T
where
    T: Deserialize<'a> + Copy,
{
    op.as_ref()
        .and_then(|map| map.get(param))
        .and_then(|x| serde_json::from_str::<T>(x).ok())
        .unwrap_or_else(|| *default)
}

/// we need to assert vec len, no other way to do it than create a func for vec
pub(crate) fn get_optional_parameter_vec<'a, T>(
    op: &'a OCP,
    param: &str,
    default: &Vec<T>,
) -> Vec<T>
where
    T: Deserialize<'a> + Clone,
{
    op.as_ref()
        .and_then(|map| map.get(param))
        .and_then(|x| serde_json::from_str::<Vec<T>>(x).ok())
        .and_then(|v| {
            if v.len() == default.len() {
                Some(v)
            } else {
                None
            }
        })
        .unwrap_or_else(|| default.clone())
}

/// we need to assert arr shape, so we have a separate func for arrs
pub(crate) fn get_optional_parameter_array<'a>(
    op: &'a OCP,
    param: &str,
    default: &Array2<f64>,
) -> Array2<f64> {
    op.as_ref()
        .and_then(|map| map.get(param))
        .and_then(|x| serde_json::from_str::<Array2<f64>>(x).ok())
        .and_then(|arr| {
            if arr.shape() == default.shape() {
                Some(arr)
            } else {
                None
            }
        })
        .unwrap_or_else(|| default.to_owned())
}

/// we need to assert arr shape, so we have a separate func for arrs
pub(crate) fn get_optional_parameter_df<'a>(
    op: &'a OCP,
    param: &str,
    default: &DataFrame,
) -> DataFrame {
    op.as_ref()
        .and_then(|map| map.get(param))
        .and_then(|x| serde_json::from_str::<DataFrame>(x).ok())
        .and_then(|arr| {
            if arr.shape() == default.shape() {
                Some(arr)
            } else {
                None
            }
        })
        .unwrap_or_else(|| default.to_owned())
}

/// Helper used to identify when to early return from the main charge function
/// This gives a view of intermediate results, such as Kb, Sb and SbAlt
pub(crate) enum ReturnMetric {
    CapitalCharge,
    Kb,
    Sb,
    #[allow(dead_code)] //TODO
    SbAlt,
    // Curvature
    KbPlus,
    KbMinus,
    // DRC
    NetLongJTD,
    NetShortJTD,
    WeightedNetLongJTD,
    WeightedNetAbsShortJTD,
    HBR
}
