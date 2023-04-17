//! RRAO module
//! RRAO is calculated at Trade Level - hence we usually need to drop duplicates
//! ,

use crate::helpers::first_appearance;
use crate::prelude::get_optional_parameter;
use crate::statics::MEDIUM_CORR_SCENARIO;
use polars::lazy::dsl::apply_multiple;
use polars::{
    df,
    prelude::{
        lit, when, ChunkFillNullValue, ChunkSet, DataType, Expr, GetOutput, IntoLazy, IntoSeries,
        Literal, NamedFrom, UniqueKeepStrategy, NULL,
    },
    series::Series,
};
use ultibi::{col, CPM};
use ultibi::{BaseMeasure, Measure, PolarsResult};

pub(crate) fn exotic_notional(_: &CPM) -> PolarsResult<Expr> {
    Ok(rrao_weighted_notional(None, "EXOTIC_RRAO"))
}

pub(crate) fn other_notional(_: &CPM) -> PolarsResult<Expr> {
    Ok(rrao_weighted_notional(None, "OTHER_RRAO"))
}

pub(crate) fn exotic_charge(op: &CPM) -> PolarsResult<Expr> {
    let exotic_weight =
        get_optional_parameter(op, "exotic_rrao_weight", &MEDIUM_CORR_SCENARIO.exotic)?;

    Ok(rrao_weighted_notional(Some(exotic_weight), "EXOTIC_RRAO"))
}

pub(crate) fn other_charge(op: &CPM) -> PolarsResult<Expr> {
    let other_weight =
        get_optional_parameter(op, "other_rrao_weight", &MEDIUM_CORR_SCENARIO.other)?;

    Ok(rrao_weighted_notional(Some(other_weight), "OTHER_RRAO"))
}

pub(crate) fn rrao_weighted_notional(weight: Option<f64>, rrao_type: &'static str) -> Expr {
    apply_multiple(
        move |columns| {
            let first_appearance_mask = first_appearance(columns[0].utf8()?);
            let rrao_type_mask = columns[1].bool()?.fill_null_with_values(false)?;

            let notional = columns[2]
                .cast(&DataType::Float64)?
                .f64()?
                .set(&(!first_appearance_mask | !rrao_type_mask), None)?;

            let res = if let Some(weight) = weight {
                notional * weight
            } else {
                notional
            };

            Ok(Some(res.into_series()))
        },
        &[col("TradeId"), col(rrao_type), col("Notional")],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

pub(crate) fn rrao_charge(op: &CPM) -> PolarsResult<Expr> {
    let exotic_weight =
        get_optional_parameter(op, "exotic_rrao_weight", &MEDIUM_CORR_SCENARIO.exotic)?;
    let other_weight =
        get_optional_parameter(op, "other_rrao_weight", &MEDIUM_CORR_SCENARIO.other)?;
    Ok(rrao_calc(exotic_weight, other_weight))
}

pub(crate) fn rrao_calc(exotic_weight: f64, other_weight: f64) -> Expr {
    apply_multiple(
        move |columns| {
            let mut df = df![
                "id"   => &columns[0],
                "e"    => &columns[1],
                "o"    => &columns[2],
                "n"    => &columns[3],
            ]?;

            df = df.unique(Some(&["id".to_string()]), UniqueKeepStrategy::First, None)?;

            df = df
                .lazy()
                .with_column(
                    when(col("e"))
                        .then(col("n") * lit(exotic_weight))
                        .when(col("o"))
                        .then(col("n") * lit(other_weight))
                        .otherwise(NULL.lit())
                        .alias("rrao"),
                )
                .collect()?;

            let res = df["rrao"].sum::<f64>().unwrap_or_else(|| 0.);
            //Ok(Series::from_vec("rrao", vec![res; columns[0].len()]))
            Ok(Some(Series::new("res", [res])))
        },
        &[
            col("TradeId"),
            col("EXOTIC_RRAO"),
            col("OTHER_RRAO"),
            col("Notional"),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

/// Exporting Measures
pub(crate) fn rrao_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "Exotic RRAO Notional".to_string(),
            calculator: std::sync::Arc::new(exotic_notional),
            aggregation: None,
            precomputefilter: Some(col("EXOTIC_RRAO").or(col("OTHER_RRAO"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Other RRAO Notional".to_string(),
            calculator: std::sync::Arc::new(other_notional),
            aggregation: None,
            precomputefilter: Some(col("EXOTIC_RRAO").or(col("OTHER_RRAO"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Exotic RRAO Charge".to_string(),
            calculator: std::sync::Arc::new(exotic_charge),
            aggregation: None,
            precomputefilter: Some(col("EXOTIC_RRAO").or(col("OTHER_RRAO"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Other RRAO Charge".to_string(),
            calculator: std::sync::Arc::new(other_charge),
            aggregation: None,
            precomputefilter: Some(col("EXOTIC_RRAO").or(col("OTHER_RRAO"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "RRAO Charge".to_string(),
            calculator: std::sync::Arc::new(rrao_charge),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("EXOTIC_RRAO").or(col("OTHER_RRAO"))),
            calc_params: vec![],
        }),
    ]
}
