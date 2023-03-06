use polars::prelude::*;
use ultibi::BaseMeasure;
use ultibi::Measure;
use ultibi::CPM;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn girr_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(girr_delta_charge_low(op)? + girr_vega_charge_low(op)? + girr_curvature_charge_low(op)?)
}

pub(crate) fn girr_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(girr_delta_charge_medium(op)?
        + girr_vega_charge_medium(op)?
        + girr_curvature_charge_medium(op)?)
}

pub(crate) fn girr_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(girr_delta_charge_high(op)? + girr_vega_charge_high(op)? + girr_curvature_charge_high(op)?)
}

pub(crate) fn girr_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "GIRR TotalCharge Low".to_string(),
            calculator: Box::new(girr_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR TotalCharge Medium".to_string(),
            calculator: Box::new(girr_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR TotalCharge High".to_string(),
            calculator: Box::new(girr_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),
        }),
    ]
}
