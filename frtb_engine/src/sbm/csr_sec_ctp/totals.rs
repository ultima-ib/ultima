use base_engine::Measure;
use base_engine::OCP;
use polars::prelude::*;

use super::delta::*;
use super::vega::*;
use super::curvature::*;
use crate::sbm::totals::total_sum3;

fn csrsecctp_total_low(op: &OCP) -> Expr {
    total_sum3(&[csr_sec_ctp_delta_charge_low(op), csrsecctp_vega_charge_low(op), csrsecctp_curvature_charge_low(op)])
}
fn csrsecctp_total_medium(op: &OCP) -> Expr {
    total_sum3(&[csr_sec_ctp_delta_charge_medium(op),csrsecctp_vega_charge_medium(op),csrsecctp_curvature_charge_medium(op)])
}
fn csrsecctp_total_high(op: &OCP) -> Expr {
    total_sum3(&[csr_sec_ctp_delta_charge_high(op), csrsecctp_vega_charge_high(op), csrsecctp_curvature_charge_high(op)])
}


pub(crate) fn csrsecctp_total_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "CSR_secCTP_TotalCharge_Low".to_string(),
            calculator: Box::new(csrsecctp_total_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
        },
        Measure {
            name: "CSR_secCTP_TotalCharge_Medium".to_string(),
            calculator: Box::new(csrsecctp_total_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),

        },
        Measure {
            name: "CSR_secCTP_TotalCharge_High".to_string(),
            calculator: Box::new(csrsecctp_total_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),

        },
    ]
}