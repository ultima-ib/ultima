use base_engine::Measure;
use base_engine::OCP;
use polars::prelude::*;

use super::delta::*;
use super::vega::*;
use super::curvature::*;
use crate::sbm::totals::total_sum;


pub(crate)fn girr_total_low(op: &OCP) -> Expr {
    total_sum(&[girr_delta_charge_low(op), girr_vega_charge_low(op), girr_curvature_charge_low(op)])
}

pub(crate)fn girr_total_medium(op: &OCP) -> Expr {
    total_sum(&[girr_delta_charge_medium(op), girr_vega_charge_medium(op), girr_curvature_charge_medium(op)])
}

pub(crate)fn girr_total_high(op: &OCP) -> Expr {
    total_sum(&[girr_delta_charge_high(op), girr_vega_charge_high(op), girr_curvature_charge_high(op)])
}

pub(crate) fn girr_total_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "GIRR_TotalCharge_Low".to_string(),
            calculator: Box::new(girr_total_low),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),
        },
        Measure {
            name: "GIRR_TotalCharge_Medium".to_string(),
            calculator: Box::new(girr_total_medium),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),

        },
        Measure {
            name: "GIRR_TotalCharge_High".to_string(),
            calculator: Box::new(girr_total_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("GIRR"))),

        },
    ]
}