use base_engine::Measure;
use base_engine::OCP;
use polars::prelude::*;

use super::delta::*;
use super::vega::*;
use super::curvature::*;
use crate::sbm::totals::total_sum;

pub(crate)fn csrsecnonctp_total_low(op: &OCP) -> Expr {
    total_sum(&[csr_sec_nonctp_delta_charge_low(op), csr_sec_nonctp_vega_charge_low(op), csr_sec_nonctp_curvature_charge_low(op)])
}
pub(crate)fn csrsecnonctp_total_medium(op: &OCP) -> Expr {
    total_sum(&[csr_sec_nonctp_delta_charge_medium(op),csr_sec_nonctp_vega_charge_medium(op),csr_sec_nonctp_curvature_charge_medium(op)])
}
pub(crate)fn csrsecnonctp_total_high(op: &OCP) -> Expr {
    total_sum(&[csr_sec_nonctp_delta_charge_high(op), csr_sec_nonctp_vega_charge_high(op), csr_sec_nonctp_curvature_charge_high(op)])
}


pub(crate) fn csrsecnonctp_total_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "CSR_Sec_nonCTP_TotalCharge_Low".to_string(),
            calculator: Box::new(csrsecnonctp_total_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),
        },
        Measure {
            name: "CSR_Sec_nonCTP_TotalCharge_Medium".to_string(),
            calculator: Box::new(csrsecnonctp_total_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),

        },
        Measure {
            name: "CSR_Sec_nonCTP_TotalCharge_High".to_string(),
            calculator: Box::new(csrsecnonctp_total_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskClass").eq(lit("FX"))),

        },
    ]
}