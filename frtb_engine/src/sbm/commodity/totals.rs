use base_engine::BaseMeasure;
use base_engine::Measure;
use base_engine::CPM;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn com_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(commodity_delta_charge_low(op)? + com_vega_charge_low(op)? + com_curvature_charge_low(op)?)
}
pub(crate) fn com_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(commodity_delta_charge_medium(op)?
        + com_vega_charge_medium(op)?
        + com_curvature_charge_medium(op)?)
}
pub(crate) fn com_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(commodity_delta_charge_high(op)?
        + com_vega_charge_high(op)?
        + com_curvature_charge_high(op)?)
}
/// Not a real measure. Used for analysis only
fn com_total_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_exprs(&[
        com_total_low(op)?,
        com_total_medium(op)?,
        com_total_high(op)?,
    ]))
}

pub(crate) fn com_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "Commodity TotalCharge Low".to_string(),
            calculator: Box::new(com_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity TotalCharge Medium".to_string(),
            calculator: Box::new(com_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity TotalCharge High".to_string(),
            calculator: Box::new(com_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity TotalCharge MAX".to_string(),
            calculator: Box::new(com_total_max),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        }),
    ]
}
