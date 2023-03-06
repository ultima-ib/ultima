use polars::prelude::*;
use ultibi::BaseMeasure;
use ultibi::Measure;
use ultibi::CPM;

use super::curvature::*;
use super::delta::*;
use super::vega::*;

pub(crate) fn csrnonsec_total_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_nonsec_delta_charge_low(op)?
        + csr_nonsec_vega_charge_low(op)?
        + csrnonsec_curvature_charge_low(op)?)
}
pub(crate) fn csrnonsec_total_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_nonsec_delta_charge_medium(op)?
        + csr_nonsec_vega_charge_medium(op)?
        + csrnonsec_curvature_charge_medium(op)?)
}
pub(crate) fn csrnonsec_total_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(csr_nonsec_delta_charge_high(op)?
        + csr_nonsec_vega_charge_high(op)?
        + csrnonsec_curvature_charge_high(op)?)
}

/// Not a real measure. Used for analysis only
fn csrnonsec_total_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_exprs(&[
        csrnonsec_total_low(op)?,
        csrnonsec_total_medium(op)?,
        csrnonsec_total_high(op)?,
    ]))
}

pub(crate) fn csrnonsec_total_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR nonSec TotalCharge Low".to_string(),
            calculator: Box::new(csrnonsec_total_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_nonSec"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec TotalCharge Medium".to_string(),
            calculator: Box::new(csrnonsec_total_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_nonSec"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec TotalCharge High".to_string(),
            calculator: Box::new(csrnonsec_total_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_nonSec"))),
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec TotalCharge MAX".to_string(),
            calculator: Box::new(csrnonsec_total_max),
            aggregation: Some("scalar"),
            precomputefilter: Some(col("RiskClass").eq(lit("CSR_nonSec"))),
        }),
    ]
}
