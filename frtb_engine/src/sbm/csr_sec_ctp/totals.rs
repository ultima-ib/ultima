use base_engine::BaseMeasure;
use base_engine::Measure;
use base_engine::CPM;
use polars::prelude::*;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn csrsecctp_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_ctp_delta_charge_low(op)?
        + csrsecctp_vega_charge_low(op)?
        + csrsecctp_curvature_charge_low(op)?)
}
pub(crate) fn csrsecctp_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_ctp_delta_charge_medium(op)?
        + csrsecctp_vega_charge_medium(op)?
        + csrsecctp_curvature_charge_medium(op)?)
}
pub(crate) fn csrsecctp_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_ctp_delta_charge_high(op)?
        + csrsecctp_vega_charge_high(op)?
        + csrsecctp_curvature_charge_high(op)?)
}

pub(crate) fn csrsecctp_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR secCTP TotalCharge Low".to_string(),
            calculator: Box::new(csrsecctp_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR secCTP TotalCharge Medium".to_string(),
            calculator: Box::new(csrsecctp_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR secCTP TotalCharge High".to_string(),
            calculator: Box::new(csrsecctp_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
        }),
    ]
}
