//! Totals across different Risk Classes
use base_engine::{Measure, OCP};
use polars::lazy::dsl::{max_exprs, Expr};

use super::commodity::totals::*;
use super::csr_nonsec::totals::*;
use super::csr_sec_ctp::totals::*;
use super::csr_sec_nonctp::totals::*;
use super::equity::totals::*;
use super::fx::totals::*;
use super::girr::totals::*;

/// Expects three Exprs corresponding to Delta, Vega, Curvature
/// TODO check if that is fixed post 23.2
/// https://github.com/pola-rs/polars/issues/4659
///
/// *`expr` to contain at least one item
//pub(crate) fn total_sum(expr: &[Expr]) -> Expr {
//    apply_multiple(
//        move |columns| {
//            let mut res = unsafe { columns.get_unchecked(0) }.fill_null(FillNullStrategy::Zero);
//            for srs in columns.iter().skip(1) {
//                res = res?.add_to(&srs.fill_null(FillNullStrategy::Zero)?)
//            }
//            res
//        },
//        expr,
//        GetOutput::from_type(DataType::Float64),
//        false,
//    )
//}

fn sbm_charge_low(op: &OCP) -> Expr {
    fx_total_low(op)
        + girr_total_low(op)
        + eq_total_low(op)
        + csrsecnonctp_total_low(op)
        + com_total_low(op)
        + csrnonsec_total_low(op)
        + csrsecctp_total_low(op)
}
fn sbm_charge_medium(op: &OCP) -> Expr {
    fx_total_medium(op)
        + girr_total_medium(op)
        + eq_total_medium(op)
        + csrsecnonctp_total_medium(op)
        + com_total_medium(op)
        + csrnonsec_total_medium(op)
        + csrsecctp_total_medium(op)
}
fn sbm_charge_high(op: &OCP) -> Expr {
    fx_total_high(op)
        + girr_total_high(op)
        + eq_total_high(op)
        + csrsecnonctp_total_high(op)
        + com_total_high(op)
        + csrnonsec_total_high(op)
        + csrsecctp_total_high(op)
}

pub(crate) fn sbm_charge(op: &OCP) -> Expr {
    max_exprs(&[
        sbm_charge_low(op),
        sbm_charge_medium(op),
        sbm_charge_high(op),
    ])
}

pub(crate) fn sbm_total_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "SBM Charge Low".to_string(),
            calculator: Box::new(sbm_charge_low),
            aggregation: Some("scalar"),
            precomputefilter: None,
        },
        Measure {
            name: "SBM Charge Medium".to_string(),
            calculator: Box::new(sbm_charge_medium),
            aggregation: Some("scalar"),
            precomputefilter: None,
        },
        Measure {
            name: "SBM Charge High".to_string(),
            calculator: Box::new(sbm_charge_high),
            aggregation: Some("scalar"),
            precomputefilter: None,
        },
        Measure {
            name: "SBM Charge".to_string(),
            calculator: Box::new(sbm_charge),
            aggregation: Some("scalar"),
            precomputefilter: None,
        },
    ]
}
