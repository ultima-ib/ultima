use base_engine::Measure;
use base_engine::OCP;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;
use crate::sbm::totals::total_sum;

pub(crate) fn eq_total_low(op: &OCP) -> Expr {
    total_sum(&[
        equity_delta_charge_low(op),
        equity_vega_charge_low(op),
        eq_curvature_charge_low(op),
    ])
}
pub(crate) fn eq_total_medium(op: &OCP) -> Expr {
    total_sum(&[
        equity_delta_charge_medium(op),
        equity_vega_charge_medium(op),
        eq_curvature_charge_medium(op),
    ])
}
pub(crate) fn eq_total_high(op: &OCP) -> Expr {
    total_sum(&[
        equity_delta_charge_high(op),
        equity_vega_charge_high(op),
        eq_curvature_charge_high(op),
    ])
}

pub(crate) fn eq_total_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "EQ_TotalCharge_Low".to_string(),
            calculator: Box::new(eq_total_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        },
        Measure {
            name: "EQ_TotalCharge_Medium".to_string(),
            calculator: Box::new(eq_total_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        },
        Measure {
            name: "EQ_TotalCharge_High".to_string(),
            calculator: Box::new(eq_total_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Equity"))),
        },
    ]
}
