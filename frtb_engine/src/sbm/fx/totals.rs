use base_engine::BaseMeasure;
use base_engine::Measure;
use base_engine::CPM;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn fx_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_delta_charge_low(op)? + fx_vega_charge_low(op)? + fx_curvature_charge_low(op)?)
}
pub(crate) fn fx_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_delta_charge_medium(op)? + fx_vega_charge_medium(op)? + fx_curvature_charge_medium(op)?)
}
pub(crate) fn fx_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_delta_charge_high(op)? + fx_vega_charge_high(op)? + fx_curvature_charge_high(op)?)
}

pub(crate) fn fx_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "FX TotalCharge Low".to_string(),
            calculator: Box::new(fx_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),
        }),
        Measure::Base(BaseMeasure {
            name: "FX TotalCharge Medium".to_string(),
            calculator: Box::new(fx_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),
        }),
        Measure::Base(BaseMeasure {
            name: "FX TotalCharge High".to_string(),
            calculator: Box::new(fx_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),
        }),
    ]
}
