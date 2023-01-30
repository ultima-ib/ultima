use base_engine::BaseMeasure;
use base_engine::Measure;
use base_engine::CPM;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn eq_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(equity_delta_charge_low(op)? + equity_vega_charge_low(op)? + eq_curvature_charge_low(op)?)
}
pub(crate) fn eq_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(equity_delta_charge_medium(op)?
        + equity_vega_charge_medium(op)?
        + eq_curvature_charge_medium(op)?)
}
pub(crate) fn eq_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(
        equity_delta_charge_high(op)?
            + equity_vega_charge_high(op)?
            + eq_curvature_charge_high(op)?,
    )
}

pub(crate) fn eq_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "EQ TotalCharge Low".to_string(),
            calculator: Box::new(eq_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        }),
        Measure::Base(BaseMeasure {
            name: "EQ TotalCharge Medium".to_string(),
            calculator: Box::new(eq_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        }),
        Measure::Base(BaseMeasure {
            name: "EQ TotalCharge High".to_string(),
            calculator: Box::new(eq_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        }),
    ]
}
