use base_engine::Measure;
use base_engine::OCP;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;
use crate::sbm::totals::total_sum;

pub(crate) fn com_total_low(op: &OCP) -> Expr {
    total_sum(&[
        commodity_delta_charge_low(op),
        com_vega_charge_low(op),
        com_curvature_charge_low(op),
    ])
}
pub(crate) fn com_total_medium(op: &OCP) -> Expr {
    total_sum(&[
        commodity_delta_charge_medium(op),
        com_vega_charge_medium(op),
        com_curvature_charge_medium(op),
    ])
}
pub(crate) fn com_total_high(op: &OCP) -> Expr {
    total_sum(&[
        commodity_delta_charge_high(op),
        com_vega_charge_high(op),
        com_curvature_charge_high(op),
    ])
}
/// Not a real measure. Used for analysis only
fn com_total_max(op: &OCP) -> Expr {
    max_exprs(&[com_total_low(op), com_total_medium(op), com_total_high(op)])
}

pub(crate) fn com_total_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "Commodity TotalCharge Low".to_string(),
            calculator: Box::new(com_total_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        },
        Measure {
            name: "Commodity TotalCharge Medium".to_string(),
            calculator: Box::new(com_total_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        },
        Measure {
            name: "Commodity TotalCharge High".to_string(),
            calculator: Box::new(com_total_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        },
        Measure {
            name: "Commodity TotalCharge MAX".to_string(),
            calculator: Box::new(com_total_max),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("Commodity"))),
        },
    ]
}
