use polars::prelude::*;
use ultibi::BaseMeasure;
use ultibi::Measure;
use ultibi::CPM;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn csrsecnonctp_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_nonctp_delta_charge_low(op)?
        + csr_sec_nonctp_vega_charge_low(op)?
        + csr_sec_nonctp_curvature_charge_low(op)?)
}
pub(crate) fn csrsecnonctp_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_nonctp_delta_charge_medium(op)?
        + csr_sec_nonctp_vega_charge_medium(op)?
        + csr_sec_nonctp_curvature_charge_medium(op)?)
}
pub(crate) fn csrsecnonctp_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_nonctp_delta_charge_high(op)?
        + csr_sec_nonctp_vega_charge_high(op)?
        + csr_sec_nonctp_curvature_charge_high(op)?)
}

fn csrsecnonctp_total_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_exprs(&[
        csrsecnonctp_total_low(op)?,
        csrsecnonctp_total_medium(op)?,
        csrsecnonctp_total_high(op)?,
    ]))
}

pub(crate) fn csrsecnonctp_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP TotalCharge Low".to_string(),
            calculator: Box::new(csrsecnonctp_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP TotalCharge Medium".to_string(),
            calculator: Box::new(csrsecnonctp_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP TotalCharge High".to_string(),
            calculator: Box::new(csrsecnonctp_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP TotalCharge MAX".to_string(),
            calculator: Box::new(csrsecnonctp_total_max),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
        }),
    ]
}
