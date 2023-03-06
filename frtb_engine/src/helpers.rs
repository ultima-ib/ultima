#![allow(clippy::unnecessary_lazy_evaluations)]

use ndarray::Array2;
use polars::prelude::{BooleanType, ChunkedArray, PolarsError, Utf8Type};
use serde::{Deserialize, Serialize};
use ultibi::{PolarsResult, CPM};

use crate::prelude::Jurisdiction;

/// if CRR2 feature is not activated, this will return BCBS
/// if jurisdiction is not part of optional params or can't parse this will return BCBS
pub(crate) fn get_jurisdiction(op: &CPM) -> PolarsResult<Jurisdiction> {
    match op.get("jurisdiction") {
        Some(j) => j
            .parse::<Jurisdiction>()
            .map_err(|_| PolarsError::ComputeError("Invalid jurisdiction".into())),
        _ => Ok(Jurisdiction::default()),
    }
}

/// Limited to types which implement Copy because this should only be used for types like f64
pub(crate) fn get_optional_parameter<'a, T>(
    op: &'a CPM,
    param: &str,
    default: &T,
) -> PolarsResult<T>
where
    T: Deserialize<'a> + Copy,
{
    match op.get(param) {
        Some(pat) => serde_json::from_str::<T>(pat).map_err(|_| {
            PolarsError::ComputeError(
                format!("Could not parse {param}: invalid value: {pat}").into(),
            )
        }),
        None => Ok(*default),
    }
}

/// we need to assert vec len, no other way to do it than create a func for vec
pub(crate) fn get_optional_parameter_vec<'a, T>(
    op: &'a CPM,
    param: &str,
    default: &Vec<T>,
) -> PolarsResult<Vec<T>>
where
    T: Deserialize<'a> + Clone,
{
    match op.get(param) {
        Some(x) => serde_json::from_str::<Vec<T>>(x)
            .map_err(|_| {
                PolarsError::ComputeError(format!("Could not parse {param} into vec: {x}").into())
            })
            .and_then(|arr| {
                if arr.len() == default.len() {
                    Ok(arr)
                } else {
                    Err(PolarsError::ComputeError(
                        format!("Invalid len of {param} vec {x}").into(),
                    ))
                }
            }),

        None => Ok(default.to_owned()),
    }
}

/// we need to assert arr shape, so we have a separate func for arrs
pub(crate) fn get_optional_parameter_array(
    op: &CPM,
    param: &str,
    default: &Array2<f64>,
) -> PolarsResult<Array2<f64>> {
    match op.get(param) {
        Some(x) => serde_json::from_str::<Array2<f64>>(x)
            .map_err(|_| {
                PolarsError::ComputeError(format!("Could not parse {param} array: {x}").into())
            })
            .and_then(|arr| {
                if arr.shape() == default.shape() {
                    Ok(arr)
                } else {
                    Err(PolarsError::ComputeError(
                        format!("Invalid shape of {param} array {x}").into(),
                    ))
                }
            }),

        None => Ok(default.to_owned()),
    }
}

/// Used for commodity Rho
pub(crate) fn get_optional_parameter_clone<'a, T>(
    op: &'a CPM,
    param: &str,
    default: &T,
) -> PolarsResult<T>
where
    T: Deserialize<'a> + Clone,
{
    match op.get(param) {
        Some(x) => serde_json::from_str::<T>(x)
            .map_err(|_| PolarsError::ComputeError(format!("Could not parse {param}: {x}").into())),
        None => Ok(default.clone()),
    }
}

/// For the cases where the requirement is completely outside of standradised approach, eg:
///
/// https://www.isda.org/a/i6MgE/Implications-of-the-FRTB-for-Carbon-Certificates.pdf
#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct RhoOverwrite {
    /// Which rho to override
    pub rhotype: RhoType,
    /// Based on what column
    pub column: String,
    /// Which column value
    pub col_equals: String,
    /// New value of the tenor
    pub value: f64,
    /// Whether both sensitivieties should satisfy value,
    /// or just one of the two.
    pub oneway: bool,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub(crate) enum RhoType {
    Tenor, // Standard Tenor rho. Usually independent
    Type,  // Same as Type/Location/Tranche (note: misleading for CSR Sec)
    Name,  // Issuer/Risk Factor. Recall, this one varies across bucket
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
    Hbr,
}

pub fn first_appearance(ca: &ChunkedArray<Utf8Type>) -> ChunkedArray<BooleanType> {
    let mut unique_values = std::collections::HashSet::new();

    ca.into_iter()
        .map(|k| unique_values.insert(k))
        .collect::<ChunkedArray<BooleanType>>()
}
